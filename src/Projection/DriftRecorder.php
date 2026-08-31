<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

use Godx\Sync\Contracts\Projector;
use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Registry\ResourceDefinition;
use Godx\Sync\Registry\SyncRegistry;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;

/**
 * Ghi CHÊNH LỆCH giữa trạng thái của Platform và trạng thái cục bộ.
 *
 * Nó KHÔNG sửa gì. Tách phát hiện khỏi sửa chữa là điều kiện để lần chạy đầu
 * tiên trên dữ liệu thật là một phép đo: nếu công cụ vừa đo vừa sửa thì con số
 * "đã sửa 4000 hàng" không phân biệt được "projector đúng, dữ liệu cũ lệch" với
 * "projector sai, vừa phá 4000 hàng".
 */
final class DriftRecorder
{
    private string $runId;

    public function __construct(
        private readonly ConnectionInterface $connection,
        private readonly SyncRegistry $registry,
    ) {
        $this->runId = (string) Str::ulid();
    }

    public function beginRun(): string
    {
        return $this->runId = (string) Str::ulid();
    }

    public function runId(): string
    {
        return $this->runId;
    }

    /** So một envelope với trạng thái cục bộ — đường của chế độ shadow. */
    public function compareOne(CloudEvent $event, Projector $projector): ?DriftKind
    {
        $local = $projector->current($event->resourceId());

        if ($event->verb() === 'deleted') {
            // Xoá ở Platform mà cục bộ vẫn còn: đó là lệch, và ở chế độ shadow
            // nó phải hiện ra chứ không lặng lẽ trôi qua.
            return $local === null
                ? null
                : $this->write($event->resourceType(), $event->resourceId(), DriftKind::OrphanLocal, null, $local, ['*deleted*']);
        }

        if ($local === null) {
            return $this->write($event->resourceType(), $event->resourceId(), DriftKind::MissingLocal, $event->data, null, array_keys($event->data));
        }

        $differing = $this->differingFields($event->resourceType(), $event->data, $local);

        if ($differing === []) {
            return null;
        }

        return $this->write($event->resourceType(), $event->resourceId(), DriftKind::FieldMismatch, $event->data, $local, $differing);
    }

    /**
     * So một hàng ảnh chụp với trạng thái cục bộ — đường của đối soát.
     *
     * @param  array<string, mixed>  $remote
     */
    public function compareSnapshotRow(string $resourceType, string $resourceId, array $remote, Projector $projector): ?DriftKind
    {
        $local = $projector->current($resourceId);

        if ($local === null) {
            return $this->write($resourceType, $resourceId, DriftKind::MissingLocal, $remote, null, array_keys($remote));
        }

        $differing = $this->differingFields($resourceType, $remote, $local);

        return $differing === []
            ? null
            : $this->write($resourceType, $resourceId, DriftKind::FieldMismatch, $remote, $local, $differing);
    }

    public function recordOrphan(string $resourceType, string $resourceId, Projector $projector): DriftKind
    {
        return $this->write($resourceType, $resourceId, DriftKind::OrphanLocal, null, $projector->current($resourceId), ['*absent-upstream*']);
    }

    /** @return array<string, int> */
    public function summary(?string $runId = null): array
    {
        return $this->connection->table('platform_sync_drift')
            ->where('run_id', $runId ?? $this->runId)
            ->selectRaw('kind, count(*) as total')
            ->groupBy('kind')
            ->pluck('total', 'kind')
            ->map(static fn (mixed $total): int => (int) $total)
            ->all();
    }

    /**
     * So sánh phần GIAO của hai bên — trường mà cả Platform lẫn consumer cùng
     * có tên.
     *
     * Bất đối xứng hai chiều, cả hai đều cố ý:
     *
     * Consumer giữ thêm cột riêng (khoá cục bộ, cột dẫn xuất, dấu thời gian) —
     * đòi hai mảng bằng nhau tuyệt đối thì mọi tài nguyên đều lệch, mãi mãi.
     *
     * Và Platform gửi thêm trường mà consumer không mirror. Cái này đắt hơn vì
     * nó KHÔNG nằm trong tay consumer: Platform thêm một cột vào payload là đủ
     * để MỌI tài nguyên loại đó thành `field_mismatch` vĩnh viễn, không ai sửa
     * được ở phía này. Một báo cáo lệch luôn đỏ thì không ai đọc — tức là mất
     * luôn công cụ, và mất đúng lúc nó cần nhất.
     *
     * Phép so từng trường mặc định NHẠY THỨ TỰ, và mặc định đó không đổi: ở
     * phần lớn payload `[a, b]` khác `[b, a]` thật. Loại nào có trường mà thứ
     * tự không mang nghĩa thì tự khai bằng `->unordered([...])`, và chỉ những
     * trường đó mới được so như tập.
     *
     * Cái KHÔNG bỏ qua: giao rỗng. Không một trường nào trùng tên nghĩa là
     * `current()` trả sai lược đồ (hợp đồng đòi "cùng khoá, cùng kiểu"), và im
     * lặng ở đó sẽ tuyên bố đồng bộ cho một projector chưa so được gì.
     *
     * @param  array<string, mixed>  $remote
     * @param  array<string, mixed>  $local
     * @return list<string>
     */
    private function differingFields(string $resourceType, array $remote, array $local): array
    {
        $unordered = $this->registry->has($resourceType)
            ? $this->registry->definition($resourceType)->unorderedFields()
            : [];

        $differing = [];
        $compared = 0;

        foreach ($remote as $key => $value) {
            if (! array_key_exists($key, $local)) {
                continue;
            }

            $compared++;

            $same = in_array((string) $key, $unordered, true)
                ? $this->sameUnordered($value, $local[$key])
                : $this->same($value, $local[$key]);

            if (! $same) {
                $differing[] = (string) $key;
            }
        }

        if ($compared === 0 && $remote !== []) {
            return array_map(static fn (int|string $key): string => (string) $key, array_keys($remote));
        }

        return $differing;
    }

    private function same(mixed $remote, mixed $local): bool
    {
        // So sánh sau khi chuẩn hoá qua JSON: một cột DB trả '1'/1/true cho
        // cùng một ý niệm tuỳ driver, và `===` trần sẽ biến sự khác biệt của
        // driver thành "lệch dữ liệu".
        if (is_scalar($remote) && is_scalar($local)) {
            if (is_bool($remote) || is_bool($local)) {
                return (bool) $remote === (bool) $local;
            }

            if (is_numeric($remote) && is_numeric($local)) {
                return (string) $remote === (string) $local || abs(((float) $remote) - ((float) $local)) < 0.000001;
            }

            return (string) $remote === (string) $local;
        }

        return json_encode($remote) === json_encode($local);
    }

    /**
     * So một trường mà consumer đã khai là KHÔNG có thứ tự
     * ({@see ResourceDefinition::unordered()}).
     *
     * Bỏ THỨ TỰ, giữ SỐ LẦN. Sắp rồi so từng phần tử nghĩa là `[a, a, b]` vẫn
     * khác `[a, b]` — một permission bị lặp là dữ liệu hỏng, và một phép so
     * "tập" theo nghĩa toán học sẽ nuốt nó im lặng. Thứ khai ở đây là "hai bên
     * liệt kê khác thứ tự", không phải "đừng nhìn kỹ".
     *
     * Khai `unordered` cho một trường không phải mảng thì không có nghĩa gì —
     * rơi về phép so thường, chứ không âm thầm thành "bằng nhau".
     */
    private function sameUnordered(mixed $remote, mixed $local): bool
    {
        if (! is_array($remote) || ! is_array($local)) {
            return $this->same($remote, $local);
        }

        return $this->sortedForCompare($remote) === $this->sortedForCompare($local);
    }

    /**
     * @param  array<array-key, mixed>  $value
     * @return list<string>
     */
    private function sortedForCompare(array $value): array
    {
        $encoded = array_map(
            static fn (mixed $item): string => (string) json_encode($item),
            array_values($value),
        );

        sort($encoded);

        return $encoded;
    }

    /**
     * @param  array<string, mixed>|null  $remote
     * @param  array<string, mixed>|null  $local
     * @param  list<string>  $differing
     */
    private function write(string $resourceType, string $resourceId, DriftKind $kind, ?array $remote, ?array $local, array $differing): DriftKind
    {
        $this->connection->table('platform_sync_drift')->insert([
            'run_id' => $this->runId,
            'resource_type' => $resourceType,
            'resource_id' => $resourceId,
            'kind' => $kind->value,
            'remote' => $remote === null ? null : json_encode($remote, JSON_THROW_ON_ERROR),
            'local' => $local === null ? null : json_encode($local, JSON_THROW_ON_ERROR),
            'differing_fields' => json_encode($differing, JSON_THROW_ON_ERROR),
            'observed_at' => Carbon::now(),
        ]);

        return $kind;
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

use Godx\Sync\Contracts\Projector;
use Godx\Sync\Envelope\CloudEvent;
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

    public function __construct(private readonly ConnectionInterface $connection)
    {
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

        $differing = $this->differingFields($event->data, $local);

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

        $differing = $this->differingFields($remote, $local);

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
     * So sánh CHỈ những trường Platform gửi.
     *
     * Consumer gần như luôn giữ thêm cột của riêng nó (khoá cục bộ, cột dẫn
     * xuất, dấu thời gian). Đòi hai mảng bằng nhau tuyệt đối sẽ báo lệch cho
     * từng tài nguyên, mãi mãi — và một báo cáo lệch luôn đỏ thì không ai đọc,
     * tức là mất luôn công cụ.
     *
     * @param  array<string, mixed>  $remote
     * @param  array<string, mixed>  $local
     * @return list<string>
     */
    private function differingFields(array $remote, array $local): array
    {
        $differing = [];

        foreach ($remote as $key => $value) {
            if (! array_key_exists($key, $local) || ! $this->same($value, $local[$key])) {
                $differing[] = (string) $key;
            }
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

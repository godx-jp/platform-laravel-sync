<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

use Godx\Sync\Contracts\Projector;
use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Inbox\Verdict;
use Godx\Sync\Registry\SyncRegistry;
use Illuminate\Contracts\Container\Container;
use Throwable;

/**
 * Đường đi của MỘT envelope, từ lúc nhận tới lúc có kết cục.
 *
 * Thứ tự các cửa là cố ý và không hoán vị được:
 *
 *   1. loại tài nguyên có đăng ký không   → không thì hỏng to tiếng, không nuốt
 *   2. đã thấy event id này chưa          → chống trùng, rẻ nhất, đặt trước
 *   3. payload đủ trường bắt buộc chưa    → chặn TRƯỚC khi chạm projector
 *   4. subject và data.id có cùng nói một tài nguyên không
 *   5. sequence có lùi không               → chặn ghi đè bằng dữ liệu cũ
 *   6. có khe hở không                     → ghi nhận, KHÔNG chặn
 *   7. shadow hay live                     → so sánh, hoặc ghi
 *
 * Cửa 3 đứng trước cửa 5 vì một payload rác cần bị từ chối kể cả khi nó mang
 * sequence mới nhất — nếu không, một thay đổi lược đồ phía Platform sẽ vừa làm
 * rỗng dữ liệu vừa đẩy vị trí lên, khiến bản sửa gửi sau bị coi là cũ.
 *
 * Cửa 4 cưỡng chế một BẤT BIẾN mà cả hệ đứng trên, và trước đó không ai kiểm:
 * mọi phép ghi sổ ở đây — `claim()`, `appliedSequence()`, `advance()`, báo cáo
 * lệch — khoá theo `resourceId()` cắt từ `subject`, trong khi projector của
 * consumer viết theo `data['id']`. Hai giá trị đó lệch nhau thì phép
 * chống-ghi-đè canh MỘT tài nguyên còn phép ghi rơi vào tài nguyên KHÁC: không
 * tầng nào ném lỗi, cả hai hàng trông bình thường sau đó, và sổ nhận ghi
 * `applied` cho một thay đổi nó chưa từng đo. Đây là dạng hỏng đắt nhất trong
 * hệ này — sai lặng, và sai theo hướng ghi đè.
 *
 * Chỉ kiểm khi payload CÓ khoá `id`. Không phải loại nào cũng đặt danh tính vào
 * `data`; bắt vạ lây ở đó là biến một rào đúng thành cổng chặn dữ liệu hợp lệ.
 */
final class EventProcessor
{
    public function __construct(
        private readonly SyncRegistry $registry,
        private readonly InboxStore $inbox,
        private readonly DriftRecorder $drift,
        private readonly Container $container,
    ) {}

    public function process(CloudEvent $event): Verdict
    {
        $definition = $this->registry->definition($event->resourceType());

        // GIÀNH trước, xử lý sau. Insert thất bại = một worker khác đã cầm
        // event này; không kiểm trước rồi ghi (xem docblock Verdict::Claimed).
        if (! $this->inbox->claim($event)) {
            return Verdict::Duplicate;
        }

        if (($missing = $this->missingFields($event, $definition->required())) !== []) {
            $this->settle($event, Verdict::Rejected, 'Payload is missing required field(s): '.implode(', ', $missing).'.');

            return Verdict::Rejected;
        }

        if (($conflict = $this->identityConflict($event)) !== null) {
            $this->settle($event, Verdict::Rejected, $conflict);

            return Verdict::Rejected;
        }

        $applied = $this->inbox->appliedSequence($event->resourceType(), $event->resourceId());

        if ($applied !== null && $event->sequence <= $applied) {
            $this->settle($event, Verdict::Stale, "Sequence {$event->sequence} is not ahead of applied sequence {$applied}.");

            return Verdict::Stale;
        }

        $gap = $this->gapNote($event, $applied);

        $projectorClass = $definition->projectorClass();

        if ($projectorClass === null) {
            $this->settle($event, Verdict::Rejected, "Resource type [{$event->resourceType()}] has no projector registered.");

            return Verdict::Rejected;
        }

        /** @var Projector $projector */
        $projector = $this->container->make($projectorClass);

        try {
            if ($definition->projectionMode()->writes()) {
                $projector->apply($event);
                $verdict = $gap === null ? Verdict::Applied : Verdict::GapNoted;
            } else {
                $this->drift->compareOne($event, $projector);
                $verdict = Verdict::Shadowed;
            }
        } catch (Throwable $e) {
            $this->settle($event, Verdict::Failed, mb_substr($e->getMessage(), 0, 1000));

            return Verdict::Failed;
        }

        $this->settle($event, $verdict, $gap);

        // Vị trí chỉ tiến khi thực sự ĐÃ GHI. Ở chế độ shadow, tiến vị trí sẽ
        // làm mọi event trở thành "cũ" ngay khi bật live — tức lần bật live đầu
        // tiên sẽ bỏ qua toàn bộ những gì đã chạy shadow, và bảng thật đứng im
        // trong khi mọi con số nói rằng đồng bộ đang chạy.
        if ($verdict->wrote()) {
            $this->inbox->advance($event);
        }

        return $verdict;
    }

    /** @param  list<string>  $required */
    private function missingFields(CloudEvent $event, array $required): array
    {
        // Event xoá không mang thân đầy đủ — đòi đủ trường ở đó là đòi Platform
        // gửi lại một bản ghi mà nó vừa bỏ đi.
        if ($event->verb() === 'deleted') {
            return [];
        }

        return array_values(array_filter(
            $required,
            static fn (string $field): bool => ! array_key_exists($field, $event->data),
        ));
    }

    /**
     * `subject` và `data['id']` phải nói cùng một tài nguyên — xem cửa 4 ở
     * docblock của lớp.
     *
     * Trả về lý do từ chối, hoặc null khi không có gì để nói (payload không
     * mang khoá `id`, hoặc hai bên khớp).
     */
    private function identityConflict(CloudEvent $event): ?string
    {
        if (! array_key_exists('id', $event->data)) {
            return null;
        }

        $payloadId = $event->data['id'];

        // Không scalar thì không so được, và đoán là sai hướng: một `id` kiểu
        // mảng/đối tượng là lỗi phía phát, không phải một danh tính khác.
        $payloadId = is_scalar($payloadId) ? (string) $payloadId : null;

        if ($payloadId === $event->resourceId()) {
            return null;
        }

        return sprintf(
            'Subject names resource [%s] but data.id is [%s]. The inbox keys dedupe and applied_sequence on the subject while the projector writes by data.id, so applying this would guard one resource and overwrite another.',
            $event->resourceId(),
            $payloadId ?? gettype($event->data['id']),
        );
    }

    private function gapNote(CloudEvent $event, ?int $applied): ?string
    {
        $previous = $event->previousSequence();

        if ($previous === null || $applied === null || $previous === $applied) {
            return null;
        }

        return "Sequence gap: event says it follows {$previous}, local position is {$applied}. Full state applied; run sync:reconcile to catch a possibly missed delete.";
    }

    private function settle(CloudEvent $event, Verdict $verdict, ?string $note): void
    {
        $this->inbox->markSettled($event->id, $verdict, $note);
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

use DateTimeImmutable;
use Godx\Sync\Contracts\Projector;
use Godx\Sync\Contracts\SnapshotsResource;
use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Exceptions\TransportFailure;
use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Transport\TransportManager;
use Illuminate\Contracts\Container\Container;
use Illuminate\Support\Str;

/**
 * Chân thứ hai của hệ: liệt kê đầy đủ rồi so với trạng thái cục bộ.
 *
 * Luồng event trả lời "có gì thay đổi". Chỉ phép liệt kê mới trả lời được câu
 * "tôi có đang thiếu thứ gì mà tôi không biết là mình thiếu không" — và đó là
 * câu duy nhất bắt được một event bị mất HẲN. Một hệ chỉ có luồng event thì
 * đúng cho tới lần rơi gói đầu tiên, và sai vĩnh viễn sau đó, không tiếng động.
 *
 * PHÁT HIỆN và SỬA tách rời. `reconcile()` mặc định chỉ ĐỌC; sửa phải vừa bật
 * live cho loại đó, vừa xin tường minh. Một công cụ vừa đo vừa sửa không phân
 * biệt được "dữ liệu cũ lệch" với "projector sai vừa phá 4000 hàng".
 */
final class Reconciler
{
    public function __construct(
        private readonly TransportManager $transports,
        private readonly SyncRegistry $registry,
        private readonly DriftRecorder $drift,
        private readonly InboxStore $inbox,
        private readonly Container $container,
    ) {}

    public function reconcile(string $resourceType, ?string $transportName = null, bool $repair = false, int $limit = 500): ReconcileResult
    {
        $transport = $this->transports->transport($transportName);

        if (! $transport instanceof SnapshotsResource) {
            throw TransportFailure::cannot($transport->name(), 'snapshotting a resource type');
        }

        $definition = $this->registry->definition($resourceType);
        $projectorClass = $definition->projectorClass();

        if ($projectorClass === null) {
            throw new \RuntimeException("Resource type [{$resourceType}] has no projector registered; there is nothing to reconcile against.");
        }

        /** @var Projector $projector */
        $projector = $this->container->make($projectorClass);

        $repairBlocked = match (true) {
            ! $repair => 'not requested',
            ! $definition->projectionMode()->writes() => "resource is in shadow mode; repairing would write to tables the mode says are off-limits. Set platform-sync.modes.{$resourceType} = live first.",
            default => null,
        };
        $repairAllowed = $repair && $repairBlocked === null;

        $runId = $this->drift->beginRun();
        $cursor = null;
        $remoteIds = [];
        $remoteCount = 0;
        $repaired = 0;

        do {
            $page = $transport->snapshot($resourceType, $cursor, $limit);

            foreach ($page->rows as $row) {
                $remoteIds[$row['id']] = true;
                $remoteCount++;

                $kind = $this->drift->compareSnapshotRow($resourceType, $row['id'], $row['data'], $projector);

                if ($kind !== null && $repairAllowed) {
                    $event = $this->synthesise($resourceType, $row);
                    $projector->apply($event);
                    $this->inbox->advance($event);
                    $repaired++;
                }
            }

            $cursor = $page->cursor;
        } while ($page->hasMore);

        // Chiều ngược: consumer còn giữ thứ Platform không còn. Duyệt id cục bộ
        // chứ không trừ tập hợp — `localIds()` là iterable để consumer có thể
        // trả về một con trỏ DB thay vì nạp cả bảng vào bộ nhớ.
        $localCount = 0;

        foreach ($projector->localIds() as $localId) {
            $localCount++;

            if (! isset($remoteIds[$localId])) {
                $this->drift->recordOrphan($resourceType, $localId, $projector);
            }
        }

        return new ReconcileResult(
            resourceType: $resourceType,
            runId: $runId,
            remoteCount: $remoteCount,
            localCount: $localCount,
            drift: $this->drift->summary($runId),
            repaired: $repaired,
            repairAllowed: $repairAllowed,
            repairBlockedReason: $repairBlocked,
        );
    }

    /**
     * Dựng một envelope tổng hợp từ hàng ảnh chụp.
     *
     * `source` nói rõ nó KHÔNG đến từ Platform qua luồng event, mà do đối soát
     * dựng ra. Giữ dấu vết đó là cách sổ nhận về sau còn phân biệt được "áp vì
     * Platform báo" với "áp vì consumer tự đi hỏi".
     *
     * @param  array{id: string, sequence: int, tenant_id: string, data: array<string, mixed>}  $row
     */
    private function synthesise(string $resourceType, array $row): CloudEvent
    {
        return new CloudEvent(
            id: (string) Str::ulid(),
            source: 'urn:godx:sync:reconcile',
            type: $resourceType.'.reconciled',
            subject: $resourceType.'/'.$row['id'],
            time: new DateTimeImmutable,
            data: $row['data'],
            sequence: $row['sequence'],
            tenantId: $row['tenant_id'],
        );
    }
}

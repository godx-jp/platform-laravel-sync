<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

use Godx\Sync\Contracts\FetchesResource;
use Godx\Sync\Contracts\PullsChanges;
use Godx\Sync\Contracts\SnapshotsResource;
use Godx\Sync\Envelope\CloudEvent;

/**
 * Transport trong bộ nhớ — cho test và cho việc dựng thử một projector mới.
 *
 * Nó cài ĐỦ ba năng lực có chủ đích: một bài test dùng driver chỉ pull được sẽ
 * không bao giờ chạm tới nhánh đối soát, và nhánh đó chính là nhánh ghi vào
 * bảng thật.
 */
final class ArrayTransport implements FetchesResource, PullsChanges, SnapshotsResource
{
    /** @var array<string, list<CloudEvent>> */
    private array $events = [];

    public function name(): string
    {
        return 'array';
    }

    public function push(CloudEvent $event): void
    {
        $this->events[$event->resourceType()][] = $event;
    }

    public function pull(string $resourceType, ?string $cursor, int $limit): ChangePage
    {
        $all = $this->events[$resourceType] ?? [];
        $offset = $cursor === null ? 0 : (int) $cursor;
        $slice = array_slice($all, $offset, $limit);
        $next = $offset + count($slice);

        return new ChangePage(
            events: array_values($slice),
            cursor: (string) $next,
            hasMore: $next < count($all),
        );
    }

    public function fetch(string $resourceType, string $resourceId): ?CloudEvent
    {
        $matching = array_values(array_filter(
            $this->events[$resourceType] ?? [],
            static fn (CloudEvent $event): bool => $event->resourceId() === $resourceId,
        ));

        if ($matching === []) {
            return null;
        }

        usort($matching, static fn (CloudEvent $a, CloudEvent $b): int => $a->sequence <=> $b->sequence);

        return end($matching) ?: null;
    }

    public function snapshot(string $resourceType, ?string $cursor, int $limit): SnapshotPage
    {
        /** @var array<string, CloudEvent> $latest */
        $latest = [];

        foreach ($this->events[$resourceType] ?? [] as $event) {
            $id = $event->resourceId();

            if (! isset($latest[$id]) || $event->sequence > $latest[$id]->sequence) {
                $latest[$id] = $event;
            }
        }

        // Tài nguyên đã xoá KHÔNG nằm trong ảnh chụp — đó là cách đối soát nhìn
        // thấy "consumer còn giữ thứ Platform đã bỏ".
        $latest = array_filter($latest, static fn (CloudEvent $event): bool => $event->verb() !== 'deleted');

        ksort($latest);
        $rows = [];

        foreach ($latest as $id => $event) {
            $rows[] = ['id' => $id, 'sequence' => $event->sequence, 'tenant_id' => $event->tenantId, 'data' => $event->data];
        }

        $offset = $cursor === null ? 0 : (int) $cursor;
        $slice = array_slice($rows, $offset, $limit);
        $next = $offset + count($slice);

        return new SnapshotPage(array_values($slice), (string) $next, $next < count($rows));
    }

    public function flush(): void
    {
        $this->events = [];
    }
}

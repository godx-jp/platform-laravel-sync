<?php

declare(strict_types=1);

namespace Godx\Sync\Inbox;

use Godx\Sync\Envelope\CloudEvent;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\QueryException;
use Illuminate\Support\Carbon;

/**
 * Sổ nhận + vị trí đã áp. Đây là ranh giới DUY NHẤT của package chạm tới DB của
 * consumer cho việc ghi sổ; mọi thứ khác đi qua `Projector` của chính consumer.
 *
 * Dùng query builder chứ không Eloquent model: model kéo theo sự kiện, observer,
 * global scope và trait của ứng dụng chủ — package không kiểm soát được cái nào
 * trong số đó, và một global scope của ứng dụng gắn vào bảng sổ nhận sẽ làm
 * phép chống trùng lặng lẽ sót.
 */
final class InboxStore
{
    public function __construct(private readonly ConnectionInterface $connection) {}

    public function alreadySeen(string $eventId): bool
    {
        return $this->connection->table('platform_sync_inbox')
            ->where('event_id', $eventId)
            ->exists();
    }

    public function appliedSequence(string $resourceType, string $resourceId): ?int
    {
        $value = $this->connection->table('platform_sync_positions')
            ->where('resource_type', $resourceType)
            ->where('resource_id', $resourceId)
            ->value('applied_sequence');

        return $value === null ? null : (int) $value;
    }

    /**
     * Ghi kết cục. Trả về false nếu event id đã tồn tại — tức một tiến trình
     * khác đã thắng cuộc đua.
     *
     * Đua thật sự xảy ra: hai worker `sync:pull` chạy song song trên cùng một
     * feed là cấu hình bình thường. Bắt lỗi trùng khoá ở đây, thay vì kiểm
     * trước rồi ghi, là cách duy nhất không có cửa sổ giữa hai bước.
     */
    public function claim(CloudEvent $event, Verdict $verdict = Verdict::Claimed, ?string $note = null): bool
    {
        try {
            $this->connection->table('platform_sync_inbox')->insert([
                'event_id' => $event->id,
                'resource_type' => $event->resourceType(),
                'resource_id' => $event->resourceId(),
                'event_type' => $event->type,
                'sequence' => $event->sequence,
                'previous_sequence' => $event->previousSequence(),
                'tenant_id' => $event->tenantId,
                'payload' => json_encode($event->toArray(), JSON_THROW_ON_ERROR),
                'verdict' => $verdict->value,
                'note' => $note,
                'received_at' => Carbon::now(),
                'settled_at' => $verdict->settled() ? Carbon::now() : null,
            ]);
        } catch (QueryException $e) {
            if ($this->isDuplicateKey($e)) {
                return false;
            }

            throw $e;
        }

        return true;
    }

    public function advance(CloudEvent $event): void
    {
        $this->connection->table('platform_sync_positions')->updateOrInsert(
            ['resource_type' => $event->resourceType(), 'resource_id' => $event->resourceId()],
            ['applied_sequence' => $event->sequence, 'last_event_id' => $event->id, 'applied_at' => Carbon::now()],
        );
    }

    public function markSettled(string $eventId, Verdict $verdict, ?string $note = null): void
    {
        $this->connection->table('platform_sync_inbox')
            ->where('event_id', $eventId)
            ->update([
                'verdict' => $verdict->value,
                'note' => $note,
                'settled_at' => $verdict->settled() ? Carbon::now() : null,
            ]);
    }

    /** @return array<string, int> */
    public function verdictCounts(string $resourceType): array
    {
        return $this->connection->table('platform_sync_inbox')
            ->where('resource_type', $resourceType)
            ->selectRaw('verdict, count(*) as total')
            ->groupBy('verdict')
            ->pluck('total', 'verdict')
            ->map(static fn (mixed $total): int => (int) $total)
            ->all();
    }

    /** @return list<array{event_id: string, resource_id: string, sequence: int, note: string|null}> */
    public function unsettled(string $resourceType, int $limit = 50): array
    {
        return $this->connection->table('platform_sync_inbox')
            ->where('resource_type', $resourceType)
            ->whereNull('settled_at')
            ->orderBy('received_at')
            ->limit($limit)
            ->get(['event_id', 'resource_id', 'sequence', 'note'])
            ->map(static fn (object $row): array => [
                'event_id' => (string) $row->event_id,
                'resource_id' => (string) $row->resource_id,
                'sequence' => (int) $row->sequence,
                'note' => $row->note === null ? null : (string) $row->note,
            ])
            ->all();
    }

    private function isDuplicateKey(QueryException $e): bool
    {
        // 23000/23505 là lớp "integrity constraint violation" của SQLSTATE —
        // dùng nó chứ không dùng mã lỗi riêng của từng driver, vì package này
        // chạy trên MySQL, Postgres và SQLite (test).
        return in_array($e->getCode(), ['23000', '23505'], true);
    }
}

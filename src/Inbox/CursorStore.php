<?php

declare(strict_types=1);

namespace Godx\Sync\Inbox;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Carbon;

/**
 * Con trỏ feed, khoá theo (transport, loại tài nguyên).
 *
 * Khoá gồm cả transport vì con trỏ là ĐỤC: `poll` có thể mã hoá nó thành offset
 * còn một driver hàng đợi mã hoá thành message id. Dùng chung một ô cho hai
 * transport nghĩa là đổi driver một lần rồi kéo nhầm đoạn, và triệu chứng sẽ là
 * "thiếu dữ liệu" chứ không phải một lỗi.
 */
final class CursorStore
{
    public function __construct(private readonly ConnectionInterface $connection) {}

    public function get(string $transport, string $resourceType): ?string
    {
        $value = $this->connection->table('platform_sync_cursors')
            ->where('transport', $transport)
            ->where('resource_type', $resourceType)
            ->value('cursor');

        return $value === null ? null : (string) $value;
    }

    public function put(string $transport, string $resourceType, ?string $cursor, int $pulled): void
    {
        $existing = (int) ($this->connection->table('platform_sync_cursors')
            ->where('transport', $transport)
            ->where('resource_type', $resourceType)
            ->value('pulled_count') ?? 0);

        $this->connection->table('platform_sync_cursors')->updateOrInsert(
            ['transport' => $transport, 'resource_type' => $resourceType],
            ['cursor' => $cursor, 'pulled_at' => Carbon::now(), 'pulled_count' => $existing + $pulled],
        );
    }

    public function forget(string $transport, string $resourceType): void
    {
        $this->connection->table('platform_sync_cursors')
            ->where('transport', $transport)
            ->where('resource_type', $resourceType)
            ->delete();
    }

    /** @return list<array{transport: string, resource_type: string, cursor: string|null, pulled_at: string|null, pulled_count: int}> */
    public function all(): array
    {
        return $this->connection->table('platform_sync_cursors')
            ->orderBy('resource_type')
            ->get()
            ->map(static fn (object $row): array => [
                'transport' => (string) $row->transport,
                'resource_type' => (string) $row->resource_type,
                'cursor' => $row->cursor === null ? null : (string) $row->cursor,
                'pulled_at' => $row->pulled_at === null ? null : (string) $row->pulled_at,
                'pulled_count' => (int) $row->pulled_count,
            ])
            ->all();
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

/**
 * Một trang trạng thái hiện tại, dùng cho đối soát.
 *
 * Khác `ChangePage` ở chỗ nó KHÔNG mang envelope: đối soát hỏi "bây giờ đang là
 * gì", không hỏi "đã có chuyện gì xảy ra". Trộn hai câu đó lại là cách người ta
 * vô tình biến một phép đối soát thành một lượt replay.
 *
 * @phpstan-type SnapshotRow array{id: string, sequence: int, tenant_id: string, data: array<string, mixed>}
 */
final readonly class SnapshotPage
{
    /** @param  list<array{id: string, sequence: int, tenant_id: string, data: array<string, mixed>}>  $rows */
    public function __construct(
        public array $rows,
        public ?string $cursor,
        public bool $hasMore,
    ) {}

    public static function empty(): self
    {
        return new self([], null, false);
    }
}

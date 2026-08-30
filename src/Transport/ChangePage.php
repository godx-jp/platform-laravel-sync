<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

use Godx\Sync\Envelope\CloudEvent;

/**
 * Một trang thay đổi cộng con trỏ để đi tiếp.
 *
 * `hasMore` là lời khai của Platform, KHÔNG suy ra từ `count($events) === $limit`.
 * Suy ra như thế sai ở đúng ranh giới: một trang đầy cuối cùng sẽ khiến consumer
 * gọi thêm một lượt rỗng mỗi chu kỳ, mãi mãi.
 */
final readonly class ChangePage
{
    /** @param  list<CloudEvent>  $events */
    public function __construct(
        public array $events,
        public ?string $cursor,
        public bool $hasMore,
    ) {}

    public static function empty(?string $cursor = null): self
    {
        return new self([], $cursor, false);
    }
}

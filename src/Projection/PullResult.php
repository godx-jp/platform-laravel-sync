<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

use Godx\Sync\Inbox\Verdict;

final readonly class PullResult
{
    /** @param  array<string, int>  $verdicts */
    public function __construct(
        public string $resourceType,
        public int $pulled,
        public array $verdicts,
        public ?string $cursor,
        public bool $hasMore,
    ) {}

    public function count(Verdict $verdict): int
    {
        return $this->verdicts[$verdict->value] ?? 0;
    }
}

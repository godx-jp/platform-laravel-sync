<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

final readonly class ReconcileResult
{
    /** @param  array<string, int>  $drift */
    public function __construct(
        public string $resourceType,
        public string $runId,
        public int $remoteCount,
        public int $localCount,
        public array $drift,
        public int $repaired,
        public bool $repairAllowed,
        public ?string $repairBlockedReason = null,
    ) {}

    public function inSync(): bool
    {
        return $this->drift === [];
    }
}

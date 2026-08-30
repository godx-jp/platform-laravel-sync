<?php

declare(strict_types=1);

namespace Godx\Sync\Console;

use Godx\Sync\Inbox\CursorStore;
use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Transport\TransportManager;
use Illuminate\Console\Command;

final class StatusCommand extends Command
{
    protected $signature = 'sync:status {--type=* : Limit to these resource types.}';

    protected $description = 'Show registered resource types, projection modes, cursors and inbox verdict counts.';

    public function handle(SyncRegistry $registry, InboxStore $inbox, CursorStore $cursors, TransportManager $transports): int
    {
        $this->components->twoColumnDetail('<fg=cyan>transport</>', $transports->defaultName());

        $types = $this->option('type') ?: $registry->types();
        $rows = [];
        $unprojected = [];

        foreach ($types as $type) {
            $definition = $registry->definition($type);
            $projector = $definition->projectorClass();

            if ($projector === null) {
                $unprojected[] = $type;
            }

            $counts = $inbox->verdictCounts($type);
            $unsettled = $inbox->unsettled($type, 1);

            $rows[] = [
                $type,
                $definition->projectionMode()->value,
                $projector === null ? '—' : class_basename($projector),
                array_sum($counts),
                $counts['applied'] ?? 0,
                $counts['shadowed'] ?? 0,
                ($counts['rejected'] ?? 0) + ($counts['failed'] ?? 0),
                $unsettled === [] ? '' : 'STUCK',
            ];
        }

        $this->table(['type', 'mode', 'projector', 'events', 'applied', 'shadowed', 'bad', 'note'], $rows);

        $cursorRows = array_map(
            static fn (array $row): array => [$row['transport'], $row['resource_type'], $row['cursor'] ?? '—', $row['pulled_at'] ?? 'never', $row['pulled_count']],
            $cursors->all(),
        );

        if ($cursorRows !== []) {
            $this->table(['transport', 'type', 'cursor', 'last pull', 'events'], $cursorRows);
        }

        if ($unprojected !== []) {
            // Loại không có projector là loại KHÔNG BAO GIỜ đồng bộ. Nó phải
            // hiện ra ở đây, vì đó là dạng hỏng duy nhất không sinh ra lỗi nào.
            $this->components->warn('No projector registered (these types will never sync): '.implode(', ', $unprojected));
        }

        return self::SUCCESS;
    }
}

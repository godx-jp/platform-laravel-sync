<?php

declare(strict_types=1);

namespace Godx\Sync\Console;

use Godx\Sync\Projection\Reconciler;
use Godx\Sync\Registry\SyncRegistry;
use Illuminate\Console\Command;

final class ReconcileCommand extends Command
{
    protected $signature = 'sync:reconcile
        {--type=* : Resource type(s) to reconcile. Omit for every projectable type.}
        {--transport= : Transport name. Must support snapshots.}
        {--repair : Write the authoritative state over local drift. Requires the type to be in live mode.}
        {--limit=500 : Snapshot page size.}';

    protected $description = 'Compare Platform state against local state and record drift. Read-only unless --repair.';

    public function handle(Reconciler $reconciler, SyncRegistry $registry): int
    {
        $types = $this->option('type') ?: $registry->projectableTypes();

        if ($types === []) {
            $this->components->error('No resource type has a projector registered; there is nothing to reconcile against.');

            return self::FAILURE;
        }

        $drifted = false;

        foreach ($types as $type) {
            $result = $reconciler->reconcile(
                resourceType: $type,
                transportName: $this->option('transport'),
                repair: (bool) $this->option('repair'),
                limit: (int) $this->option('limit'),
            );

            $this->components->twoColumnDetail("<fg=cyan>{$type}</>", "run {$result->runId}");
            $this->components->twoColumnDetail('  remote / local', "{$result->remoteCount} / {$result->localCount}");

            foreach ($result->drift as $kind => $count) {
                $drifted = true;
                $this->components->twoColumnDetail("  <fg=yellow>{$kind}</>", (string) $count);
            }

            if ($result->inSync()) {
                $this->components->twoColumnDetail('  drift', '<fg=green>none</>');
            }

            if ($result->repairBlockedReason !== null && $result->repairBlockedReason !== 'not requested') {
                // Đòi sửa mà bị chặn thì phải NÓI, không được im lặng bỏ qua:
                // người vận hành vừa gõ --repair và sẽ tin rằng nó đã chạy.
                $this->components->warn("  --repair ignored: {$result->repairBlockedReason}");
            }

            if ($result->repaired > 0) {
                $this->components->twoColumnDetail('  <fg=red>repaired</>', (string) $result->repaired);
            }
        }

        // Có lệch KHÔNG phải lỗi của lệnh — đó là kết quả nó sinh ra để tìm.
        // Nhưng nó phải phân biệt được với "không lệch" ở mã thoát, vì đây là
        // thứ một job có lịch sẽ treo cảnh báo lên.
        return $drifted ? 2 : self::SUCCESS;
    }
}

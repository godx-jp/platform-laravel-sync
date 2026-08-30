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
        {--limit= : Snapshot page size.}
        {--max-pages= : Safety cap on snapshot pages per type per run.}';

    protected $description = 'Compare Platform state against local state and record drift. Read-only unless --repair.';

    public function handle(Reconciler $reconciler, SyncRegistry $registry): int
    {
        $types = $this->option('type') ?: $registry->projectableTypes();

        if ($types === []) {
            $this->components->error('No resource type has a projector registered; there is nothing to reconcile against.');

            return self::FAILURE;
        }

        $drifted = false;
        $incomplete = false;

        foreach ($types as $type) {
            $result = $reconciler->reconcile(
                resourceType: $type,
                transportName: $this->option('transport'),
                repair: (bool) $this->option('repair'),
                limit: (int) ($this->option('limit') ?: config('platform-sync.reconcile.page_size', 500)),
                maxPages: (int) ($this->option('max-pages') ?: config('platform-sync.reconcile.max_pages', 200)),
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

            if (! $result->complete) {
                // "Không thấy lệch" trên nửa ảnh chụp KHÔNG phải "không lệch".
                // Im ở đây thì lượt chạy thoát 0 và người đọc đóng cảnh báo
                // dựa trên một phép đo chưa chạy xong.
                $incomplete = true;
                $this->components->warn("  incomplete: {$result->incompleteReason}");
            }
        }

        // Có lệch KHÔNG phải lỗi của lệnh — đó là kết quả nó sinh ra để tìm.
        // Nhưng nó phải phân biệt được với "không lệch" ở mã thoát, vì đây là
        // thứ một job có lịch sẽ treo cảnh báo lên.
        // Lượt đọc dở KHÔNG được thoát 0: mã thoát này là thứ một job có lịch
        // đọc, và 0 ở đó nghĩa là "đã đối soát xong, sạch".
        if ($incomplete) {
            return self::FAILURE;
        }

        return $drifted ? 2 : self::SUCCESS;
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Console;

use Godx\Sync\Contracts\SnapshotsResource;
use Godx\Sync\Projection\Reconciler;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Transport\TransportManager;
use Illuminate\Console\Command;
use Throwable;

final class ReconcileCommand extends Command
{
    protected $signature = 'sync:reconcile
        {--type=* : Resource type(s) to reconcile. Omit for every projectable type.}
        {--transport= : Transport name. Must support snapshots.}
        {--repair : Write the authoritative state over local drift. Requires the type to be in live mode.}
        {--limit= : Snapshot page size.}
        {--max-pages= : Safety cap on snapshot pages per type per run.}';

    protected $description = 'Compare Platform state against local state and record drift. Read-only unless --repair.';

    public function handle(Reconciler $reconciler, SyncRegistry $registry, TransportManager $transports): int
    {
        $types = $this->option('type') ?: $registry->projectableTypes();

        if ($types === []) {
            $this->components->error('No resource type has a projector registered; there is nothing to reconcile against.');

            return self::FAILURE;
        }

        $transportName = $this->snapshotTransportName();

        if (($refusal = $this->refuseUnlessItCanSnapshot($transports, $transportName)) !== null) {
            foreach ($refusal as $line) {
                $this->components->error($line);
            }

            return self::FAILURE;
        }

        $drifted = false;
        $incomplete = false;

        foreach ($types as $type) {
            $result = $reconciler->reconcile(
                resourceType: $type,
                transportName: $transportName,
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

    /**
     * `--transport` → `platform-sync.reconcile.transport` → mặc định.
     *
     * Nấc giữa tồn tại vì kiến trúc của ADR 0002 là "sự kiện qua SQS, ảnh chụp
     * qua HTTP": bắt người vận hành gõ `--transport=poll` mỗi lần chạy nghĩa là
     * một job có lịch quên gõ nó sẽ đỏ mãi mãi, và cách sửa nhanh nhất khi đó
     * là gỡ luôn job đối soát — tức gỡ đúng chân duy nhất bắt được event bị mất.
     */
    private function snapshotTransportName(): ?string
    {
        $option = $this->option('transport');

        if (is_string($option) && $option !== '') {
            return $option;
        }

        $configured = config('platform-sync.reconcile.transport');

        return is_string($configured) && $configured !== '' ? $configured : null;
    }

    /**
     * Nói bằng tiếng người khi transport không chụp được, thay vì để
     * `TransportFailure` nổi lên thành stack trace.
     *
     * `Reconciler` vẫn ném — rào đó phải ở lại, vì nó bảo vệ mọi người gọi khác
     * chứ không riêng lệnh này. Nhưng người vận hành gõ `sync:reconcile` trên
     * một hệ chạy SQS thì gặp đúng tình huống này ở lượt đầu tiên, mỗi lần cài
     * mới, và một stack trace ở đó không nói được điều duy nhất cần nói: hàng
     * đợi KHÔNG liệt kê được trạng thái hiện tại, và đó không phải lỗi cấu hình
     * của ai cả.
     *
     * @return list<string>|null
     */
    private function refuseUnlessItCanSnapshot(TransportManager $transports, ?string $name): ?array
    {
        try {
            $transport = $transports->transport($name);
        } catch (Throwable) {
            // Tên sai / chưa khai: `Reconciler` sẽ ném đúng thông điệp của
            // `UnknownTransport`, và nó đã nêu tên khoá cấu hình còn thiếu.
            return null;
        }

        if ($transport instanceof SnapshotsResource) {
            return null;
        }

        $capable = $this->snapshotCapableNames($transports);
        $suggestion = $capable === [] ? 'poll' : $capable[0];

        return [
            sprintf(
                'Transport [%s] cannot list Platform state, and a full listing is exactly what reconciliation is. A queue only ever hands you what changed, so it can never answer "am I missing something I do not know I am missing".',
                $transport->name(),
            ),
            sprintf('Run it against a snapshot-capable transport: sync:reconcile --transport=%s', $suggestion),
            sprintf(
                'Or keep events on [%s] and point only snapshots at HTTP: set platform-sync.reconcile.transport (PLATFORM_SYNC_RECONCILE_TRANSPORT) to %s.',
                $transport->name(),
                $suggestion,
            ),
            'Configured transports that can snapshot: '.($capable === [] ? 'none' : implode(', ', $capable)).'.',
        ];
    }

    /** @return list<string> */
    private function snapshotCapableNames(TransportManager $transports): array
    {
        $capable = [];

        foreach ($transports->names() as $name) {
            try {
                if ($transports->transport($name) instanceof SnapshotsResource) {
                    $capable[] = $name;
                }
            } catch (Throwable) {
                // Một transport khai thiếu cấu hình không được làm hỏng câu trả
                // lời về các transport khác.
            }
        }

        return $capable;
    }
}

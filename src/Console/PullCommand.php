<?php

declare(strict_types=1);

namespace Godx\Sync\Console;

use Godx\Sync\Inbox\Verdict;
use Godx\Sync\Projection\FeedPuller;
use Godx\Sync\Registry\SyncRegistry;
use Illuminate\Console\Command;

final class PullCommand extends Command
{
    protected $signature = 'sync:pull
        {--type=* : Resource type(s) to pull. Omit to pull every type that has a projector.}
        {--transport= : Transport name from platform-sync.transports. Defaults to the configured one.}
        {--limit= : Page size.}
        {--max-pages= : Safety cap on pages per type per run.}';

    protected $description = 'Pull resource changes from Platform and run them through the inbox.';

    public function handle(FeedPuller $puller, SyncRegistry $registry): int
    {
        $types = $this->option('type') ?: $registry->projectableTypes();

        if ($types === []) {
            // Không có projector nào nghĩa là chưa ai nối package này vào bảng
            // nào. Trả về THÀNH CÔNG với một dòng giải thích sẽ dựng nên một hệ
            // trông như đang chạy mà không tài nguyên nào tới đích.
            $this->components->error('No resource type has a projector registered. Register one with SyncRegistry::resource(...)->projector(...).');

            return self::FAILURE;
        }

        $rows = [];
        $failed = false;

        foreach ($types as $type) {
            $result = $puller->pull(
                resourceType: $type,
                transportName: $this->option('transport'),
                limit: (int) ($this->option('limit') ?: config('platform-sync.pull.page_size', 200)),
                maxPages: (int) ($this->option('max-pages') ?: config('platform-sync.pull.max_pages', 50)),
            );

            $failedCount = $result->count(Verdict::Failed) + $result->count(Verdict::Rejected);
            $failed = $failed || $failedCount > 0;

            $rows[] = [
                $type,
                $result->pulled,
                $result->count(Verdict::Applied),
                $result->count(Verdict::Shadowed),
                $result->count(Verdict::Duplicate) + $result->count(Verdict::Stale),
                $result->count(Verdict::GapNoted),
                $failedCount,
                $result->hasMore ? 'yes' : 'no',
            ];
        }

        $this->table(['type', 'pulled', 'applied', 'shadowed', 'skipped', 'gaps', 'failed', 'more'], $rows);

        // Rejected/Failed làm lệnh ĐỎ. Một lượt kéo nuốt lỗi rồi thoát 0 là
        // cách chắc chắn nhất để không ai biết đồng bộ đã hỏng từ tuần trước.
        return $failed ? self::FAILURE : self::SUCCESS;
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

use Godx\Sync\Contracts\PullsChanges;
use Godx\Sync\Exceptions\TransportFailure;
use Godx\Sync\Inbox\CursorStore;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Transport\TransportManager;

/**
 * Vòng kéo: con trỏ → trang → xử lý từng envelope → lưu con trỏ.
 *
 * Con trỏ chỉ tiến SAU khi cả trang đã có kết cục. Tiến trước sẽ biến một lần
 * chết giữa trang thành mất dữ liệu vĩnh viễn — lần chạy sau bắt đầu từ sau
 * đoạn chưa xử lý xong. Ngược lại, tiến sau thì lần chạy lại sẽ kéo trùng, và
 * trùng thì lưới chống trùng ở inbox nuốt gọn. Chọn hướng hỏng có sửa được.
 */
final class FeedPuller
{
    public function __construct(
        private readonly TransportManager $transports,
        private readonly SyncRegistry $registry,
        private readonly CursorStore $cursors,
        private readonly EventProcessor $processor,
    ) {}

    public function pull(string $resourceType, ?string $transportName = null, int $limit = 200, int $maxPages = 50): PullResult
    {
        $transport = $this->transports->transport($transportName);
        $name = $transport->name();

        if (! $transport instanceof PullsChanges) {
            throw TransportFailure::cannot($name, 'pulling changes');
        }

        // Loại chưa đăng ký thì hỏng ở ĐÂY, trước khi chạm mạng — thông điệp
        // nêu đúng nguyên nhân, thay vì một lỗi phân giải sâu trong vòng lặp.
        $this->registry->definition($resourceType);

        $cursor = $this->cursors->get($name, $resourceType);
        $verdicts = [];
        $pulled = 0;
        $hasMore = false;
        $pages = 0;

        do {
            $page = $transport->pull($resourceType, $cursor, $limit);

            foreach ($page->events as $event) {
                $verdict = $this->processor->process($event);
                $verdicts[$verdict->value] = ($verdicts[$verdict->value] ?? 0) + 1;
                $pulled++;
            }

            $cursor = $page->cursor;
            $this->cursors->put($name, $resourceType, $cursor, count($page->events));
            $hasMore = $page->hasMore;
            $pages++;

            // Trần số trang là cố ý: một feed đang chạy có thể sinh event nhanh
            // hơn tốc độ kéo, và vòng lặp "cho tới khi hết" sẽ không bao giờ
            // kết thúc. Kết thúc rồi chạy lại là hành vi đúng cho một tiến
            // trình có lịch.
        } while ($hasMore && $pages < $maxPages);

        return new PullResult($resourceType, $pulled, $verdicts, $cursor, $hasMore);
    }
}

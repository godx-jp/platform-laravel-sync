<?php

declare(strict_types=1);

namespace Godx\Sync\Contracts;

/**
 * Transport mà việc NHẬN và việc XOÁ là hai bước tách rời.
 *
 * Một feed HTTP không cần cái này: con trỏ tiến lên là toàn bộ hành động "đã
 * xong", và nó nằm trong tay consumer. Hàng đợi thì ngược — message còn nằm đó
 * cho tới khi ai đó xoá nó, và ĐÚNG thời điểm xoá là thứ quyết định hệ mất dữ
 * liệu hay không:
 *
 *   xoá TRONG `pull()`      → tiến trình chết giữa lúc chiếu ⇒ event bốc hơi
 *                              vĩnh viễn, không dấu vết ở đâu cả
 *   xoá SAU khi có kết cục  → tiến trình chết ⇒ message hiện lại, giao lần hai,
 *                              lưới chống trùng ở sổ nhận nuốt gọn
 *
 * Hướng hỏng thứ hai sửa được, hướng thứ nhất thì không. Nên `pull()` KHÔNG
 * được xoá, và ai gọi `pull()` phải nói lại cho transport biết khi nào xong.
 *
 * Vì sao là một interface RIÊNG chứ không thêm phương thức vào `PullsChanges`:
 * cùng lý do docblock của `Transport` đã ghi — `PollTransport` không có gì để
 * ack, và một `ack()` rỗng trong nó là một lời nói dối biên dịch được. Năng lực
 * khai bằng interface con, người gọi kiểm `instanceof`.
 */
interface AcknowledgesDelivery extends Transport
{
    /**
     * Envelope đã có KẾT CỤC trong sổ nhận — được phép xoá khỏi hàng đợi.
     *
     * "Kết cục" nghĩa là `Verdict::settled()`, tức mọi thứ trừ `Failed` và
     * `Claimed`. `Rejected` cũng nằm trong đó có chủ đích: một envelope sai
     * lược đồ sẽ sai y hệt ở lần giao kế, và sổ nhận đã giữ nguyên văn nó cùng
     * lý do từ chối — để nó quay lại hàng đợi chỉ là trả cùng một câu trả lời
     * thêm bốn lần rồi rơi vào dead-letter.
     *
     * PHẢI im lặng bỏ qua một `$eventId` mà transport không cầm biên nhận: đối
     * soát tự dựng envelope (`urn:godx:sync:reconcile`) chưa bao giờ đi qua
     * hàng đợi nào.
     */
    public function ack(string $eventId): void;

    /**
     * Envelope CHƯA có kết cục — trả nó về hàng đợi.
     *
     * Cố ý KHÔNG rút visibility timeout về 0. Trả ngay tức là quay vòng nóng
     * trên đúng cái vừa hỏng: cùng một payload, cùng một projector, cùng một
     * lỗi, vài chục lượt mỗi giây, cho tới khi chạm `maxReceiveCount`. Để
     * visibility timeout tự hết hạn là cơ chế lùi có sẵn của hàng đợi — dùng
     * nó, đừng dựng lại.
     */
    public function abandon(string $eventId, string $reason): void;
}

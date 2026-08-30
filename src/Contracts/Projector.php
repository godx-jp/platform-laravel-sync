<?php

declare(strict_types=1);

namespace Godx\Sync\Contracts;

use Godx\Sync\Envelope\CloudEvent;

/**
 * Cầu nối giữa một loại tài nguyên của Platform và bảng của consumer.
 *
 * Package CỐ Ý không ship projector cụ thể nào. Tempo cất chi nhánh ở bảng
 * `branches` với khoá ngoại của riêng nó; consumer khác cất khác. Một projector
 * "dùng chung" sẽ phải đoán lược đồ của người khác, và cái giá của đoán sai ở
 * đây là ghi đè dữ liệu đang chạy.
 *
 * Package chỉ định nghĩa TỪ VỰNG (loại tài nguyên, hình dạng payload) và MÁY
 * MÓC (chống trùng, thứ tự, shadow, đối soát).
 */
interface Projector
{
    /**
     * Trạng thái cục bộ ĐÃ CHUẨN HOÁ của một tài nguyên, hoặc null nếu chưa có.
     *
     * "Chuẩn hoá" nghĩa là cùng khoá, cùng kiểu với `data` của envelope — vì
     * chế độ shadow và báo cáo lệch so sánh trực tiếp hai mảng này. Trả về cột
     * DB thô (tên khác, kiểu khác) sẽ làm mọi tài nguyên trông như đang lệch.
     *
     * @return array<string, mixed>|null
     */
    public function current(string $resourceId): ?array;

    /**
     * Áp trạng thái quyền uy. PHẢI idempotent — cùng một event áp hai lần cho
     * cùng một kết quả.
     *
     * Giao hàng là at-least-once, không phải exactly-once: lưới chống trùng ở
     * inbox chặn phần lớn bản sao, nhưng nó không chặn được lần áp bị ngắt giữa
     * chừng rồi thử lại. Exactly-once không tồn tại trên một đường mạng; thứ
     * tồn tại là at-least-once cộng với một hàm idempotent.
     */
    public function apply(CloudEvent $event): void;

    /**
     * Mọi id của loại này mà consumer đang giữ.
     *
     * Dùng để phát hiện chiều NGƯỢC của lệch: tài nguyên Platform đã xoá nhưng
     * consumer vẫn giữ. Chiều đó không bao giờ lộ ra qua luồng event nếu chính
     * event xoá là cái bị mất.
     *
     * @return iterable<int, string>
     */
    public function localIds(): iterable;
}

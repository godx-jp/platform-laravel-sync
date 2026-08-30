<?php

declare(strict_types=1);

namespace Godx\Sync\Inbox;

/**
 * Kết cục của MỘT envelope. Mọi envelope đều được ghi lại kèm kết cục — kể cả
 * cái bị bỏ.
 *
 * Vì sao ghi cả cái bị bỏ: "vì sao chi nhánh này không cập nhật" chỉ trả lời
 * được nếu hệ nhớ rằng nó ĐÃ nhận event đó và đã quyết định bỏ, cùng lý do.
 * Bỏ im lặng thì câu hỏi đó không có đáy.
 */
enum Verdict: string
{
    /**
     * Đã GIÀNH được quyền xử lý, chưa có kết cục.
     *
     * Hàng này được ghi TRƯỚC khi projector chạy, không phải sau. Kiểm-rồi-ghi
     * để lại một cửa sổ giữa hai bước, và hai worker `sync:pull` chạy song song
     * trên cùng feed là cấu hình bình thường — cả hai sẽ qua cửa kiểm rồi cùng
     * gọi projector. Ghi trước biến ràng buộc khoá chính của DB thành phép loại
     * trừ nguyên tử: đúng một worker thắng.
     *
     * Hàng còn kẹt ở `claimed` nghĩa là tiến trình chết giữa chừng. Nó KHÔNG tự
     * biến mất — `sync:status` phải nêu ra, vì đó là công việc dở dang duy nhất
     * mà hệ không tự nhận ra được.
     */
    case Claimed = 'claimed';

    /** Đã áp vào bảng thật. */
    case Applied = 'applied';

    /** Chế độ shadow: đã so sánh, đã ghi lệch (nếu có), KHÔNG ghi bảng. */
    case Shadowed = 'shadowed';

    /** Đã thấy đúng event id này rồi. Giao hàng at-least-once là bình thường. */
    case Duplicate = 'duplicate';

    /** sequence ≤ vị trí đã áp — event đến muộn, áp vào sẽ LÙI dữ liệu. */
    case Stale = 'stale';

    /**
     * Đã áp, NHƯNG có khe hở trong `sequence` — thiếu một đoạn lịch sử.
     *
     * Vẫn áp, và đây không phải nhân nhượng: envelope mang TRẠNG THÁI ĐẦY ĐỦ
     * (event-carried state transfer), nên bản mới nhất đã chứa mọi thứ mà các
     * bản bị mất từng nói. Đi kéo lại đoạn đã mất chỉ tốn một vòng mạng để tới
     * đúng cái kết quả này.
     *
     * Ghi lại vì MỘT trường hợp nó vẫn mất thật: event bị mất là event XOÁ cuối
     * cùng của tài nguyên. Khi ấy không có bản nào tới sau để lộ ra khe hở, và
     * consumer giữ lại một hàng đáng lẽ phải biến mất. Chỉ đối soát bắt được nó
     * — nên khe hở là tín hiệu gọi đối soát, không phải tín hiệu chặn hàng đợi.
     */
    case GapNoted = 'gap_noted';

    /** Payload không đạt rào tối thiểu của registry. */
    case Rejected = 'rejected';

    /** Projector ném lỗi. */
    case Failed = 'failed';

    public function settled(): bool
    {
        return $this !== self::Failed && $this !== self::Claimed;
    }

    /** Kết cục có ĐỔI trạng thái cục bộ không. */
    public function wrote(): bool
    {
        return $this === self::Applied || $this === self::GapNoted;
    }
}

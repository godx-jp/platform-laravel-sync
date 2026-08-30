<?php

declare(strict_types=1);

namespace Godx\Sync\Registry;

/**
 * Shadow là MẶC ĐỊNH, và đó là quyết định thiết kế chứ không phải giá trị khởi
 * tạo cho tiện.
 *
 * Projector ghi vào chính những bảng mà ứng dụng đang phục vụ khách. Một
 * projector sai không ném lỗi — nó ghi giá trị sai và mọi thứ vẫn xanh. Chế độ
 * shadow biến lần chạy đầu tiên thành một PHÉP ĐO (lệch bao nhiêu, ở đâu) thay
 * vì một canh bạc, và nó không tốn gì ngoài một nhịp.
 */
enum ProjectionMode: string
{
    /** Chỉ ĐỌC trạng thái cục bộ và ghi báo cáo lệch. Không ghi bảng nào. */
    case Shadow = 'shadow';

    /** Ghi thật qua projector. */
    case Live = 'live';

    public function writes(): bool
    {
        return $this === self::Live;
    }
}

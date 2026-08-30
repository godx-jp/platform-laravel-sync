<?php

declare(strict_types=1);

namespace Godx\Sync\Directory;

use Godx\Sync\Registry\SyncRegistry;

/**
 * TỪ VỰNG tài nguyên của Platform — và chỉ từ vựng.
 *
 * Package cố ý KHÔNG ship projector cho các loại này. Tempo cất chi nhánh ở
 * bảng `branches` với khoá ngoại riêng, consumer khác cất khác; một projector
 * "dùng chung" sẽ phải đoán lược đồ của người khác, và giá của đoán sai ở đây là
 * ghi đè dữ liệu đang chạy.
 *
 * Cái package sở hữu là: tên loại (để hai bên gọi cùng một thứ bằng cùng một
 * tên), và tập trường TỐI THIỂU mà payload phải có (để một thay đổi lược đồ phía
 * Platform không lặng lẽ làm rỗng một cột).
 *
 * TỪ VỰNG AUTHZ KHÔNG nằm ở đây, và đó không phải mâu thuẫn: permission/role/
 * role_binding đi CHUNG đường ống này (chúng là tài nguyên có id, có phiên bản,
 * có chủ sở hữu — hệt một chi nhánh), nhưng CHỦ SỞ HỮU của từ vựng ấy là
 * `platform-laravel-auth`, vì chính nó là thứ diễn giải chúng thành quyền.
 *
 * Ranh giới đó có giá trị đo được: một consumer chỉ cần org/branch sẽ không phải
 * kéo theo tầng Gate/middleware của authz, và một thay đổi trong từ vựng quyền
 * không đụng tới package này.
 */
final class DirectoryResources
{
    public const ORGANIZATION = 'godx.directory.organization';

    public const BRAND = 'godx.directory.brand';

    public const BRANCH = 'godx.directory.branch';

    public const EMPLOYEE = 'godx.directory.employee';

    /**
     * Trường TỐI THIỂU của payload, theo một luật duy nhất: **cột nào bên
     * consumer là NOT NULL và không có default thì trường đó bắt buộc.**
     *
     * Luật này chọn chỗ HỎNG. Trường thiếu mà có tên ở đây bị chặn ở cửa 3 với
     * `Verdict::Rejected` và một câu nói rõ thiếu gì; trường thiếu mà KHÔNG có
     * tên ở đây đi hết đường tới projector rồi đổ ở tầng SQL thành
     * `Verdict::Failed`, mang thông điệp của driver DB — cùng một sự cố, báo
     * cáo ở chỗ khó đọc nhất.
     *
     * `slug` là ca đúng của luật đó: `organizations.slug` · `brands.slug` ·
     * `branches.slug` đều NOT NULL, không default, và có unique index. Một
     * event `created` thiếu nó chắc chắn đổ — câu hỏi chỉ là đổ ở đâu.
     *
     * `is_active` thì KHÔNG có tên ở đây dù nó tồn tại ở cả ba bảng: nó có
     * default (`true`), nên payload thiếu nó vẫn ghi được một hàng đúng.
     *
     * TỪ VỰNG CỜ HOẠT ĐỘNG là `is_active` (boolean), KHÔNG phải `status`. Bản
     * trước đòi `status` cho organization và đó là một trường KHÔNG bên nào
     * dùng: consumer khai `organizations.is_active` (boolean, default true), và
     * payload của Platform mang `is_active` ở nơi nó mang cờ này (brand) —
     * không một feed nào gửi `status`. Một trường bắt buộc mà bên phát không
     * bao giờ gửi thì mọi envelope loại đó bị từ chối, mãi mãi, với một lý do
     * đọc lên nghe như lỗi của Platform. `status` còn tệ hơn `is_active` ở một
     * điểm nữa: nó là chuỗi mở, package không định nghĩa tập giá trị, nên rào
     * "có mặt" là rào duy nhất viết được — trong khi boolean thì đúng hai giá
     * trị và ánh xạ thẳng vào cột.
     *
     * `timezone` của branch là ngoại lệ CÓ CHỦ ĐÍCH: cột cục bộ nullable, mà
     * trường vẫn bắt buộc. Business date, ranh ca và mọi báo cáo theo ngày phân
     * giải từ nó; NULL không nổ, nó chỉ âm thầm đánh sai ngày cho cả một chi
     * nhánh. Ở đây một envelope bị từ chối rẻ hơn một hàng được ghi.
     *
     * @var array<string, list<string>>
     */
    private const REQUIRED = [
        self::ORGANIZATION => ['id', 'name', 'slug'],
        self::BRAND => ['id', 'organization_id', 'name', 'slug'],
        self::BRANCH => ['id', 'organization_id', 'name', 'slug', 'timezone'],
        self::EMPLOYEE => ['id', 'organization_id', 'email'],
    ];

    /** @return list<string> */
    public static function all(): array
    {
        return array_keys(self::REQUIRED);
    }

    /** @return list<string> */
    public static function requiredFor(string $type): array
    {
        return self::REQUIRED[$type] ?? [];
    }

    /**
     * Khai mọi loại của Platform vào registry.
     *
     * Chỉ khai TỪ VỰNG. Loại nào chưa có projector thì `sync:status` nêu ra và
     * `sync:pull` từ chối thẳng — im lặng bỏ qua sẽ tạo ra một hệ trông như
     * đang chạy trong khi không tài nguyên nào tới đích.
     */
    public static function register(SyncRegistry $registry): void
    {
        foreach (self::REQUIRED as $type => $required) {
            $registry->resource($type)->requires($required);
        }
    }
}

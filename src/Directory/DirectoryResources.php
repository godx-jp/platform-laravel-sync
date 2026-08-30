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
 * `authz` nằm ở đây cùng `directory` chứ không phải một hệ riêng: một role
 * binding là một tài nguyên có id, có sequence, có chủ sở hữu — hệt như một chi
 * nhánh. Dựng một đường ống riêng cho nó là nhân đôi máy móc để phục vụ một
 * khác biệt không tồn tại.
 */
final class DirectoryResources
{
    public const ORGANIZATION = 'godx.directory.organization';

    public const BRAND = 'godx.directory.brand';

    public const BRANCH = 'godx.directory.branch';

    public const EMPLOYEE = 'godx.directory.employee';

    public const PERMISSION = 'godx.authz.permission';

    public const ROLE = 'godx.authz.role';

    public const ROLE_BINDING = 'godx.authz.role_binding';

    /** @var array<string, list<string>> */
    private const REQUIRED = [
        self::ORGANIZATION => ['id', 'name', 'status'],
        self::BRAND => ['id', 'organization_id', 'name'],
        self::BRANCH => ['id', 'organization_id', 'name', 'timezone'],
        self::EMPLOYEE => ['id', 'organization_id', 'email'],
        self::PERMISSION => ['id', 'slug'],
        self::ROLE => ['id', 'slug', 'permissions'],
        // `branch_id` PHẢI có mặt, kể cả khi null: null nghĩa là MỌI chi nhánh,
        // không phải "không chi nhánh nào". Vắng khoá và khoá mang null là hai
        // chuyện khác nhau, và gộp chúng lại là cách một binding toàn tổ chức
        // biến thành binding không phạm vi.
        self::ROLE_BINDING => ['id', 'user_id', 'role_id', 'organization_id', 'branch_id'],
    ];

    /** @return list<string> */
    public static function all(): array
    {
        return array_keys(self::REQUIRED);
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

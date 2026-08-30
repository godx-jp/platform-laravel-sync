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

    /** @var array<string, list<string>> */
    private const REQUIRED = [
        self::ORGANIZATION => ['id', 'name', 'status'],
        self::BRAND => ['id', 'organization_id', 'name'],
        self::BRANCH => ['id', 'organization_id', 'name', 'timezone'],
        self::EMPLOYEE => ['id', 'organization_id', 'email'],
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

<?php

declare(strict_types=1);

namespace Godx\Sync\Registry;

use Godx\Sync\Exceptions\UnknownResourceType;

/**
 * Bản đồ loại tài nguyên → projector, do các package domain khai lúc boot.
 *
 * Registry cố ý KHÔNG tự khám phá bằng cách quét thư mục. Quét thì một class
 * đặt sai chỗ sẽ im lặng đứng ngoài, và "vì sao tài nguyên này không bao giờ
 * đồng bộ" là câu hỏi tốn nhiều giờ nhất trong loại hệ này. Khai tường minh thì
 * thiếu là thấy ngay ở `sync:status`.
 */
final class SyncRegistry
{
    /** @var array<string, ResourceDefinition> */
    private array $definitions = [];

    public function resource(string $type): ResourceDefinition
    {
        return $this->definitions[$type] ??= new ResourceDefinition($type);
    }

    public function definition(string $type): ResourceDefinition
    {
        return $this->definitions[$type]
            ?? throw UnknownResourceType::make($type, array_keys($this->definitions));
    }

    public function has(string $type): bool
    {
        return isset($this->definitions[$type]);
    }

    /** @return list<string> */
    public function types(): array
    {
        $types = array_keys($this->definitions);
        sort($types);

        return $types;
    }

    /** @return list<string> */
    public function projectableTypes(): array
    {
        return array_values(array_filter(
            $this->types(),
            fn (string $type): bool => $this->definitions[$type]->projectorClass() !== null,
        ));
    }

    /**
     * Áp chế độ từ cấu hình ứng dụng.
     *
     * Package khai mặc định (shadow); ứng dụng có quyền nâng lên live cho từng
     * loại. Chiều này — cấu hình THẮNG mã nguồn — là cố ý: bật ghi thật là
     * quyết định vận hành, và nó phải nhìn thấy được ở một file cấu hình chứ
     * không nằm rải trong các lời gọi `->mode()`.
     *
     * @param  array<string, string>  $modes
     */
    public function applyModes(array $modes): void
    {
        foreach ($modes as $type => $mode) {
            if (! isset($this->definitions[$type])) {
                continue;
            }

            $resolved = ProjectionMode::tryFrom($mode);

            // Chế độ viết sai chính tả KHÔNG được âm thầm thành live. Giữ
            // shadow là hướng hỏng an toàn.
            if ($resolved !== null) {
                $this->definitions[$type]->mode($resolved);
            }
        }
    }
}

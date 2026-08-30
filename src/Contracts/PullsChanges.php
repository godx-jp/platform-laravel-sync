<?php

declare(strict_types=1);

namespace Godx\Sync\Contracts;

use Godx\Sync\Transport\ChangePage;

/**
 * Transport kéo được một trang thay đổi theo con trỏ.
 *
 * Con trỏ là ĐỤC (opaque) với consumer — không được suy diễn nó là timestamp
 * hay số thứ tự, vì Platform có quyền đổi cách mã hoá. Consumer chỉ lưu lại
 * chuỗi và trả nguyên văn ở lần gọi sau.
 */
interface PullsChanges extends Transport
{
    public function pull(string $resourceType, ?string $cursor, int $limit): ChangePage;
}

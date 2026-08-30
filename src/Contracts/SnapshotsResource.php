<?php

declare(strict_types=1);

namespace Godx\Sync\Contracts;

use Godx\Sync\Transport\SnapshotPage;

/**
 * Liệt kê TOÀN BỘ tài nguyên của một loại — chân đối soát.
 *
 * Luồng event là đường NHANH, không phải đường ĐÚNG. Chỉ có phép liệt kê đầy
 * đủ mới trả lời được câu "consumer có đang thiếu thứ gì mà nó không biết là
 * mình thiếu không" — và câu đó là câu duy nhất phát hiện được event bị mất
 * hẳn.
 */
interface SnapshotsResource extends Transport
{
    public function snapshot(string $resourceType, ?string $cursor, int $limit): SnapshotPage;
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Tests\Fixtures;

use Godx\Sync\Contracts\SnapshotsResource;
use Godx\Sync\Transport\SnapshotPage;
use RuntimeException;

/**
 * Một Platform nói `has_more: true` MÃI MÃI.
 *
 * Không phải giả định xa vời: một feed đang chạy sinh hàng nhanh hơn tốc độ đọc
 * cho đúng hành vi đó, và một lỗi phân trang phía Platform cũng vậy.
 *
 * Nó tự nổ sau `$explodeAfter` trang thay vì quay vòng vô hạn — một bài test đo
 * "có trần hay không" mà lại TREO khi trần vắng mặt thì không đo được gì: nó
 * chỉ làm cả suite đứng, và không ai đọc được kết quả của một tiến trình bị
 * giết.
 */
final class EndlessSnapshotTransport implements SnapshotsResource
{
    public int $calls = 0;

    public function __construct(public readonly int $explodeAfter = 200) {}

    public function name(): string
    {
        return 'endless';
    }

    public function snapshot(string $resourceType, ?string $cursor, int $limit): SnapshotPage
    {
        $this->calls++;

        if ($this->calls > $this->explodeAfter) {
            throw new RuntimeException("Snapshot loop ran {$this->calls} pages without a cap.");
        }

        return new SnapshotPage(
            rows: [[
                'id' => 'remote_'.$this->calls,
                'sequence' => $this->calls,
                'tenant_id' => 'org_1',
                'data' => ['id' => 'remote_'.$this->calls, 'name' => 'Widget '.$this->calls],
            ]],
            cursor: (string) $this->calls,
            hasMore: true,
        );
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

final readonly class ReconcileResult
{
    /**
     * @param  array<string, int>  $drift
     * @param  bool  $complete  Ảnh chụp đã đọc HẾT chưa. False = lượt này đọc
     *                          nửa chừng, nên mọi kết luận rút ra từ phép trừ
     *                          tập hợp (chiều `orphan_local`) đã bị bỏ.
     */
    public function __construct(
        public string $resourceType,
        public string $runId,
        public int $remoteCount,
        public int $localCount,
        public array $drift,
        public int $repaired,
        public bool $repairAllowed,
        public ?string $repairBlockedReason = null,
        public bool $complete = true,
        public ?string $incompleteReason = null,
    ) {}

    /**
     * "Hai bên bằng nhau" — và một lượt đọc DỞ không được phép trả lời câu này.
     *
     * Không lệch nào được ghi trên nửa ảnh chụp không chứng minh được điều gì
     * về nửa còn lại; trả `true` ở đó là biến một phép đo dở dang thành một lời
     * xác nhận, và người đọc sẽ đóng cảnh báo dựa trên nó.
     */
    public function inSync(): bool
    {
        return $this->complete && $this->drift === [];
    }
}

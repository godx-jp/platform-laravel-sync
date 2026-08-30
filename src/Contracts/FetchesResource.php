<?php

declare(strict_types=1);

namespace Godx\Sync\Contracts;

use Godx\Sync\Envelope\CloudEvent;

/**
 * Kéo trạng thái HIỆN TẠI của đúng một tài nguyên.
 *
 * Đây là đường thoát cho khe hở thứ tự (`sequence` nhảy cóc). Không có nó,
 * consumer gặp khe hở chỉ còn hai lựa chọn: chờ mãi một event có thể đã nằm
 * trong dead-letter, hoặc áp event mới và bỏ qua khoảng đã mất — cái đầu là
 * treo, cái sau là mất dữ liệu im lặng.
 */
interface FetchesResource extends Transport
{
    public function fetch(string $resourceType, string $resourceId): ?CloudEvent;
}

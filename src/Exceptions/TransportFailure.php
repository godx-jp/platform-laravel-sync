<?php

declare(strict_types=1);

namespace Godx\Sync\Exceptions;

use RuntimeException;

final class TransportFailure extends RuntimeException
{
    public static function http(string $transport, string $what, int $status): self
    {
        return new self("Transport [{$transport}] returned HTTP {$status} for [{$what}].");
    }

    /**
     * 304 ở một feed mà consumer KHÔNG hỏi điều kiện.
     *
     * Có riêng một thông điệp vì mặc định thì lỗi này nói dối: 304 < 400 nên
     * `failed()` là false, request đi thẳng vào `json()` của một thân rỗng, và
     * cái nổi lên là "non-JSON body" — đổ lỗi cho thân phản hồi trong khi
     * nguyên nhân là mã trạng thái. Người đọc sẽ đi tìm một body hỏng không tồn
     * tại.
     */
    public static function unexpectedNotModified(string $transport, string $what): self
    {
        return new self("Transport [{$transport}] returned HTTP 304 for [{$what}], but no If-None-Match was sent. Platform must answer this feed with full rows.");
    }

    public static function body(string $transport, string $what): self
    {
        return new self("Transport [{$transport}] returned a non-JSON body for [{$what}].");
    }

    public static function cannot(string $transport, string $capability): self
    {
        return new self("Transport [{$transport}] does not support [{$capability}]. Configure a transport that implements it, or run the command with a driver that does.");
    }
}

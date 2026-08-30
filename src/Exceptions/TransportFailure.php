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

    public static function body(string $transport, string $what): self
    {
        return new self("Transport [{$transport}] returned a non-JSON body for [{$what}].");
    }

    public static function cannot(string $transport, string $capability): self
    {
        return new self("Transport [{$transport}] does not support [{$capability}]. Configure a transport that implements it, or run the command with a driver that does.");
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Exceptions;

use InvalidArgumentException;

final class UnknownTransport extends InvalidArgumentException
{
    public static function notConfigured(string $name): self
    {
        return new self("Sync transport [{$name}] is not configured. Add it under [platform-sync.transports].");
    }

    public static function noDriver(string $driver, string $name): self
    {
        return new self("Sync transport [{$name}] asks for driver [{$driver}], which no package or application has registered. Register it with PlatformSync::extend('{$driver}', ...).");
    }
}

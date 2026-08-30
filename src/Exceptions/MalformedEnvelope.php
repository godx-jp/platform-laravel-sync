<?php

declare(strict_types=1);

namespace Godx\Sync\Exceptions;

use RuntimeException;

final class MalformedEnvelope extends RuntimeException
{
    public static function specVersion(string $seen): self
    {
        return new self("Envelope specversion must be 1.0, saw [{$seen}].");
    }

    public static function missing(string $field): self
    {
        return new self("Envelope is missing required field [{$field}].");
    }

    public static function sequence(string $seen): self
    {
        return new self("Envelope sequence must be a non-negative integer, saw [{$seen}].");
    }

    public static function time(string $seen): self
    {
        return new self("Envelope time is not a valid RFC 3339 timestamp: [{$seen}].");
    }
}

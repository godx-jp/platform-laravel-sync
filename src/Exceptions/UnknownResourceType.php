<?php

declare(strict_types=1);

namespace Godx\Sync\Exceptions;

use InvalidArgumentException;

final class UnknownResourceType extends InvalidArgumentException
{
    /** @param  list<string>  $known */
    public static function make(string $type, array $known): self
    {
        sort($known);
        $list = $known === [] ? 'none registered' : implode(', ', $known);

        return new self("Resource type [{$type}] is not registered. Known types: {$list}.");
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Tests\Fixtures;

use DateTimeImmutable;
use Godx\Sync\Envelope\CloudEvent;
use Illuminate\Support\Str;

final class Envelopes
{
    public const TYPE = 'godx.test.widget';

    /** @param  array<string, mixed>  $data */
    public static function make(
        string $id,
        int $sequence,
        array $data = [],
        string $verb = 'updated',
        ?int $previousSequence = null,
        ?string $eventId = null,
    ): CloudEvent {
        return new CloudEvent(
            id: $eventId ?? (string) Str::ulid(),
            source: 'https://id.godx.jp',
            type: self::TYPE.'.'.$verb,
            subject: self::TYPE.'/'.$id,
            time: new DateTimeImmutable('2026-08-31T00:00:00Z'),
            data: $data === [] ? ['id' => $id, 'name' => 'Widget '.$id] : $data,
            sequence: $sequence,
            tenantId: 'org_1',
            extensions: $previousSequence === null ? [] : ['prevsequence' => (string) $previousSequence],
        );
    }
}

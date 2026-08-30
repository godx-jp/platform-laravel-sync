<?php

declare(strict_types=1);

use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Exceptions\MalformedEnvelope;

function envelopeArray(array $overrides = []): array
{
    return array_merge([
        'specversion' => '1.0',
        'id' => '01JQ8F2K0000000000000000AA',
        'source' => 'https://id.godx.jp',
        'type' => 'godx.directory.branch.updated',
        'subject' => 'godx.directory.branch/br_1',
        'time' => '2026-08-31T04:12:07.000000Z',
        'sequence' => '48211',
        'tenantid' => 'org_1',
        'data' => ['id' => 'br_1', 'name' => 'Hongo'],
    ], $overrides);
}

it('parses a well-formed CloudEvent', function (): void {
    $event = CloudEvent::fromArray(envelopeArray());

    expect($event->resourceType())->toBe('godx.directory.branch')
        ->and($event->resourceId())->toBe('br_1')
        ->and($event->verb())->toBe('updated')
        ->and($event->sequence)->toBe(48211)
        ->and($event->tenantId)->toBe('org_1');
});

it('rejects an envelope whose specversion is not 1.0', function (): void {
    CloudEvent::fromArray(envelopeArray(['specversion' => '0.3']));
})->throws(MalformedEnvelope::class, 'saw [0.3]');

it('names the field that is missing', function (string $field): void {
    $payload = envelopeArray();
    unset($payload[$field]);

    CloudEvent::fromArray($payload);
})->with(['id', 'source', 'type', 'subject', 'time', 'sequence', 'tenantid', 'data'])
    ->throws(MalformedEnvelope::class);

it('refuses a non-numeric sequence instead of coercing it to zero', function (): void {
    // Ép về 0 sẽ TẮT phép kiểm thứ tự cho toàn bộ tài nguyên đó mà không báo
    // gì — mọi event sau đó trông như cũ hơn vị trí đã áp.
    CloudEvent::fromArray(envelopeArray(['sequence' => 'forty-eight']));
})->throws(MalformedEnvelope::class, 'non-negative integer');

it('accepts an integer sequence as well as a string one', function (): void {
    expect(CloudEvent::fromArray(envelopeArray(['sequence' => 48211]))->sequence)->toBe(48211);
});

it('reads prevsequence out of the extensions', function (): void {
    $event = CloudEvent::fromArray(envelopeArray(['prevsequence' => '48210']));

    expect($event->previousSequence())->toBe(48210);
});

it('treats a missing prevsequence as "gap detection unavailable", not as zero', function (): void {
    // Trả 0 sẽ khiến mọi event đầu tiên của một tài nguyên trông như có khe hở.
    expect(CloudEvent::fromArray(envelopeArray())->previousSequence())->toBeNull();
});

it('survives a subject with no slash', function (): void {
    $event = CloudEvent::fromArray(envelopeArray(['subject' => 'br_1']));

    expect($event->resourceId())->toBe('br_1');
});

it('round-trips through toArray without losing the required extensions', function (): void {
    $original = CloudEvent::fromArray(envelopeArray(['prevsequence' => '48210']));
    $again = CloudEvent::fromArray($original->toArray());

    expect($again->sequence)->toBe($original->sequence)
        ->and($again->previousSequence())->toBe(48210)
        ->and($again->tenantId)->toBe($original->tenantId);
});

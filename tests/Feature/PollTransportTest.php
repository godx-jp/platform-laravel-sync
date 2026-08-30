<?php

declare(strict_types=1);

use Godx\Sync\Exceptions\TransportFailure;
use Godx\Sync\Transport\PollTransport;
use Illuminate\Http\Client\Factory as HttpFactory;
use Illuminate\Support\Facades\Http;

function poll(): PollTransport
{
    return new PollTransport(app(HttpFactory::class), ['endpoint' => 'https://id.godx.jp/sync', 'token' => 'secret', 'retries' => 1]);
}

function envelopeBody(string $id = 'br_1', int $sequence = 5): array
{
    return [
        'specversion' => '1.0',
        'id' => '01JQ8F2K0000000000000000AA',
        'source' => 'https://id.godx.jp',
        'type' => 'godx.directory.branch.updated',
        'subject' => "godx.directory.branch/{$id}",
        'time' => '2026-08-31T04:12:07Z',
        'sequence' => (string) $sequence,
        'tenantid' => 'org_1',
        'data' => ['id' => $id, 'name' => 'Hongo'],
    ];
}

it('sends the bearer token and returns parsed envelopes', function (): void {
    Http::fake(['*' => Http::response(['events' => [envelopeBody()], 'cursor' => 'c2', 'has_more' => false])]);

    $page = poll()->pull('godx.directory.branch', null, 100);

    expect($page->events)->toHaveCount(1)
        ->and($page->cursor)->toBe('c2');

    Http::assertSent(fn ($request): bool => $request->hasHeader('Authorization', 'Bearer secret'));
});

it('keeps the cursor on a 304 instead of rewinding the feed', function (): void {
    // Trả `null` ở đây sẽ làm consumer quay về đầu feed và kéo lại toàn bộ lịch
    // sử ở chu kỳ kế — mỗi lần Platform nói "chưa có gì mới".
    Http::fake(['*' => Http::response('', 304)]);

    $page = poll()->pull('godx.directory.branch', 'c9', 100);

    expect($page->events)->toBe([])
        ->and($page->cursor)->toBe('c9');
});

it('sends If-None-Match once it has seen an ETag', function (): void {
    Http::fakeSequence()
        ->push(['events' => [], 'cursor' => 'c1', 'has_more' => false], 200, ['ETag' => 'W/"abc"'])
        ->push('', 304);

    $transport = poll();
    $transport->pull('godx.directory.branch', null, 100);
    $transport->pull('godx.directory.branch', 'c1', 100);

    Http::assertSent(fn ($request): bool => $request->hasHeader('If-None-Match', 'W/"abc"'));
});

it('treats a 404 on fetch as "deleted upstream", not as a failure', function (): void {
    // Ném lỗi ở đây biến một khe hở giải quyết được thành một hàng đợi tắc.
    Http::fake(['*' => Http::response('', 404)]);

    expect(poll()->fetch('godx.directory.branch', 'br_gone'))->toBeNull();
});

it('raises the status code when the feed itself is broken', function (): void {
    Http::fake(['*' => Http::response('', 503)]);

    poll()->pull('godx.directory.branch', null, 100);
})->throws(TransportFailure::class, 'HTTP 503');

it('rejects a non-JSON body rather than silently syncing nothing', function (): void {
    Http::fake(['*' => Http::response('<html>maintenance</html>', 200)]);

    poll()->pull('godx.directory.branch', null, 100);
})->throws(TransportFailure::class, 'non-JSON body');

it('reads a snapshot page', function (): void {
    Http::fake(['*' => Http::response([
        'rows' => [['id' => 'br_1', 'sequence' => 5, 'tenant_id' => 'org_1', 'data' => ['id' => 'br_1']]],
        'cursor' => 'c2',
        'has_more' => true,
    ])]);

    $page = poll()->snapshot('godx.directory.branch', null, 100);

    expect($page->rows)->toHaveCount(1)
        ->and($page->rows[0]['sequence'])->toBe(5)
        ->and($page->hasMore)->toBeTrue();
});

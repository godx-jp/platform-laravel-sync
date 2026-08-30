<?php

declare(strict_types=1);

use Godx\Sync\Contracts\Transport;
use Godx\Sync\Exceptions\TransportFailure;
use Godx\Sync\Exceptions\UnknownResourceType;
use Godx\Sync\Inbox\CursorStore;
use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Inbox\Verdict;
use Godx\Sync\Projection\FeedPuller;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\TransportManager;

beforeEach(function (): void {
    FakeProjector::reset();

    app(SyncRegistry::class)
        ->resource(Envelopes::TYPE)
        ->projector(FakeProjector::class)
        ->requires(['id', 'name'])
        ->mode(ProjectionMode::Live);

    $this->transport = new ArrayTransport;
    app(TransportManager::class)->set('array', $this->transport);
});

it('pulls a feed, applies every event and remembers the cursor', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));
    $this->transport->push(Envelopes::make('w2', 1));

    $result = app(FeedPuller::class)->pull(Envelopes::TYPE);

    expect($result->pulled)->toBe(2)
        ->and($result->count(Verdict::Applied))->toBe(2)
        ->and(FakeProjector::$state)->toHaveKeys(['w1', 'w2'])
        ->and(app(CursorStore::class)->get('array', Envelopes::TYPE))->toBe('2');
});

it('does not reapply events when the same feed is pulled again from the start', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));
    app(FeedPuller::class)->pull(Envelopes::TYPE);

    // Con trỏ bị mất (khôi phục DB, đổi máy) là chuyện có thật; lưới chống
    // trùng phải đứng độc lập với con trỏ, không phải dựa vào nó.
    app(CursorStore::class)->forget('array', Envelopes::TYPE);
    $result = app(FeedPuller::class)->pull(Envelopes::TYPE);

    expect($result->count(Verdict::Duplicate))->toBe(1)
        ->and(FakeProjector::$applied)->toHaveCount(1);
});

it('walks more than one page', function (): void {
    foreach (range(1, 5) as $i) {
        $this->transport->push(Envelopes::make("w{$i}", 1));
    }

    $result = app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 2);

    expect($result->pulled)->toBe(5)
        ->and($result->hasMore)->toBeFalse();
});

it('stops at the page cap instead of chasing a feed that keeps growing', function (): void {
    foreach (range(1, 10) as $i) {
        $this->transport->push(Envelopes::make("w{$i}", 1));
    }

    $result = app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 2, maxPages: 2);

    expect($result->pulled)->toBe(4)
        ->and($result->hasMore)->toBeTrue();
});

it('advances the cursor only after the page has been processed', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));
    FakeProjector::$throwOnApply = true;

    app(FeedPuller::class)->pull(Envelopes::TYPE);

    // Con trỏ VẪN tiến, và điều đó là cố ý: event đã được ghi vào sổ nhận với
    // kết cục `failed`, nên nó không mất — nó nằm trong danh sách chưa yên.
    // Giữ con trỏ đứng im sẽ kéo lại vô hạn cùng một event hỏng và chặn mọi
    // event sau nó.
    expect(app(CursorStore::class)->get('array', Envelopes::TYPE))->toBe('1')
        ->and(app(InboxStore::class)->unsettled(Envelopes::TYPE))->toHaveCount(1);
});

it('names the transport when it cannot pull', function (): void {
    $mute = new class implements Transport
    {
        public function name(): string
        {
            return 'mute';
        }
    };

    app(TransportManager::class)->set('mute', $mute);

    app(FeedPuller::class)->pull(Envelopes::TYPE, transportName: 'mute');
})->throws(TransportFailure::class, 'does not support [pulling changes]');

it('fails on an unregistered resource type before it touches the network', function (): void {
    app(FeedPuller::class)->pull('godx.test.nope');
})->throws(UnknownResourceType::class, 'not registered');

<?php

declare(strict_types=1);

use Godx\Sync\Inbox\CursorStore;
use Godx\Sync\Projection\FeedPuller;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Godx\Sync\Tests\Fixtures\FakeSqs;
use Godx\Sync\Transport\SqsTransport;
use Godx\Sync\Transport\TransportManager;

beforeEach(function (): void {
    FakeProjector::reset();

    $this->fake = new FakeSqs;
    $this->transport = new SqsTransport($this->fake->client, ['queue_url' => 'https://sqs/tempo-identity-events']);

    config()->set('platform-sync.default', 'sqs');
    app(TransportManager::class)->set('sqs', $this->transport);

    app(SyncRegistry::class)
        ->resource(Envelopes::TYPE)
        ->projector(FakeProjector::class)
        ->requires(['id', 'name'])
        ->mode(ProjectionMode::Live);
});

function queued(array $envelope, string $receipt = 'rh-1', string $messageId = 'msg-1'): array
{
    return [
        'MessageId' => $messageId,
        'ReceiptHandle' => $receipt,
        'Body' => json_encode($envelope, JSON_THROW_ON_ERROR),
    ];
}

it('deletes a message only after the inbox has settled it', function (): void {
    $this->fake->willReceive([queued(Envelopes::make('w1', 1)->toArray(), 'rh-w1')]);

    $result = app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 10);

    expect($result->pulled)->toBe(1)
        ->and(FakeProjector::$state)->toHaveKey('w1')
        ->and($this->fake->callsOf('DeleteMessage')[0]['ReceiptHandle'])->toBe('rh-w1');
});

it('leaves a message on the queue when the projector blew up', function (): void {
    // `Failed` KHÔNG settled: projector đổ có thể là sự cố nhất thời (DB đầy,
    // deadlock), và hàng đợi đã có sẵn thử lại kèm dead-letter. Xoá ở đây là
    // vứt đi một thay đổi thật rồi báo cáo màu xanh ở lượt sau.
    FakeProjector::$throwOnApply = true;
    $this->fake->willReceive([queued(Envelopes::make('w1', 1)->toArray(), 'rh-w1')]);

    app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 10);

    expect($this->fake->countOf('DeleteMessage'))->toBe(0);
});

it('deletes a rejected envelope, because it will be just as wrong next time', function (): void {
    // Payload thiếu trường bắt buộc. Sổ nhận đã giữ nguyên văn nó cùng lý do từ
    // chối, nên trả nó về hàng đợi chỉ là nghe lại cùng một câu bốn lần nữa rồi
    // rơi vào DLQ.
    $this->fake->willReceive([queued(Envelopes::make('w1', 1, ['id' => 'w1'])->toArray(), 'rh-bad')]);

    app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 10);

    expect($this->fake->callsOf('DeleteMessage')[0]['ReceiptHandle'])->toBe('rh-bad');
});

it('deletes a duplicate, which is what at-least-once delivery looks like', function (): void {
    // Giao hàng hai lần là hình dạng bình thường khi visibility timeout hết hạn
    // giữa lúc xử lý — không phải sự cố, và bản thứ hai phải rời hàng đợi.
    $envelope = Envelopes::make('w1', 1, eventId: 'evt-same')->toArray();
    $this->fake
        ->willReceive([queued($envelope, 'rh-first')])
        ->willReceive([queued($envelope, 'rh-second', 'msg-2')]);

    app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 10);
    app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 10);

    expect($this->fake->countOf('DeleteMessage'))->toBe(2)
        ->and($this->fake->callsOf('DeleteMessage')[1]['ReceiptHandle'])->toBe('rh-second')
        ->and(FakeProjector::$applied)->toHaveCount(1);
});

it('keeps a null sqs cursor from rewinding the poll feed', function (): void {
    // `null` của `sqs` nghĩa là "không có vị trí nào để nhớ"; `null` của `poll`
    // nghĩa là "quay về đầu feed". Hai nghĩa trái ngược sống chung được CHỈ VÌ
    // `CursorStore` khoá theo (transport, loại tài nguyên).
    $cursors = app(CursorStore::class);

    $cursors->put('poll', Envelopes::TYPE, 'c9', 3);
    $cursors->put('sqs', Envelopes::TYPE, null, 1);

    expect($cursors->get('poll', Envelopes::TYPE))->toBe('c9')
        ->and($cursors->get('sqs', Envelopes::TYPE))->toBeNull();
});

it('does not let an unregistered resource type kill the consumption run', function (): void {
    // Hệ quả trực tiếp của "một hàng đợi chở mọi loại": Platform có quyền phát
    // một loại mà consumer NÀY không chiếu (service thứ hai đăng ký sau, hoặc
    // một loại mới ra đời trước khi consumer kịp cập nhật). Envelope đó không
    // được làm đổ cả lượt tiêu thụ — `EventProcessor::process()` NÉM
    // `UnknownResourceType`, nên nó không bao giờ được đưa vào trang.
    $unknown = Envelopes::make('x1', 1)->toArray();
    $unknown['type'] = 'godx.unknown.thing.updated';
    $unknown['subject'] = 'godx.unknown.thing/x1';
    $unknown['id'] = 'evt-unknown';

    $this->fake->willReceive([
        queued(Envelopes::make('w1', 1)->toArray(), 'rh-good', 'msg-good'),
        queued($unknown, 'rh-unknown', 'msg-unknown'),
    ]);

    $result = app(FeedPuller::class)->pull(Envelopes::TYPE, limit: 10);

    expect($result->pulled)->toBe(1)
        ->and(FakeProjector::$state)->toHaveKey('w1')
        // Envelope lành đã xoá; envelope không ai chiếu thì KHÔNG xoá — nó quay
        // lại, đếm lên, rồi vào dead-letter. Ồn ào, và đó là điều đúng: xoá im
        // lặng biến "consumer này không hiểu loại đó" thành không dấu vết nào.
        ->and($this->fake->countOf('DeleteMessage'))->toBe(1)
        ->and($this->fake->callsOf('DeleteMessage')[0]['ReceiptHandle'])->toBe('rh-good');
});

it('runs sync:pull green when the queue also holds a type nobody projects', function (): void {
    $unknown = Envelopes::make('x1', 1)->toArray();
    $unknown['type'] = 'godx.unknown.thing.updated';
    $unknown['subject'] = 'godx.unknown.thing/x1';
    $unknown['id'] = 'evt-unknown';

    $this->fake->willReceive([
        queued(Envelopes::make('w1', 1)->toArray(), 'rh-good', 'msg-good'),
        queued($unknown, 'rh-unknown', 'msg-unknown'),
    ]);

    $this->artisan('sync:pull --type='.Envelopes::TYPE)->assertExitCode(0);
});

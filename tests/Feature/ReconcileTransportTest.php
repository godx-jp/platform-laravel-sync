<?php

declare(strict_types=1);

use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\TransportManager;

/**
 * Chân đối soát vẫn cần HTTP — SQS không liệt kê được trạng thái hiện tại.
 *
 * ADR 0002 đặt đối soát ở bước 2 và transport ở bước 3, đúng theo thứ tự đó, vì
 * đối soát là PHÉP ĐO chứng minh bước 3 hoạt động. Nên một hệ đã chuyển sang
 * SQS mà `sync:reconcile` nổ stack trace ở lượt đầu tiên là một hệ mà người
 * vận hành sẽ gỡ luôn job đối soát — tức gỡ đúng chân duy nhất bắt được event
 * bị mất hẳn.
 */
beforeEach(function (): void {
    FakeProjector::reset();

    $this->transport = new ArrayTransport;
    app(TransportManager::class)->set('array', $this->transport);

    app(SyncRegistry::class)
        ->resource(Envelopes::TYPE)
        ->projector(FakeProjector::class)
        ->requires(['id', 'name'])
        ->mode(ProjectionMode::Live);

    // Cấu hình đúng hình dạng ADR: sự kiện qua SQS.
    config()->set('platform-sync.default', 'sqs');
    config()->set('platform-sync.transports.sqs', [
        'driver' => 'sqs',
        'queue_url' => 'https://sqs/tempo-identity-events',
        'region' => 'ap-northeast-1',
    ]);
});

it('explains itself instead of throwing when the transport cannot snapshot', function (): void {
    $this->artisan('sync:reconcile')
        ->expectsOutputToContain('cannot list Platform state')
        ->expectsOutputToContain('--transport=')
        ->expectsOutputToContain('PLATFORM_SYNC_RECONCILE_TRANSPORT')
        ->assertExitCode(1);
});

it('names the transports that can actually snapshot', function (): void {
    $this->artisan('sync:reconcile')
        ->expectsOutputToContain('Configured transports that can snapshot:')
        ->expectsOutputToContain('poll')
        ->assertExitCode(1);
});

it('still reconciles when the operator names a snapshot-capable transport', function (): void {
    // Chiều ngược: câu giải thích KHÔNG được biến thành một cổng chặn cả lệnh.
    $this->transport->push(Envelopes::make('w1', 1));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    $this->artisan('sync:reconcile --transport=array')->assertExitCode(0);
});

it('lets configuration split the legs: events over sqs, snapshots over http', function (): void {
    // Bắt gõ `--transport=` mỗi lần chạy nghĩa là một job có lịch quên gõ nó sẽ
    // đỏ mãi mãi, và cách sửa nhanh nhất khi đó là gỡ luôn job đối soát.
    config()->set('platform-sync.reconcile.transport', 'array');

    $this->transport->push(Envelopes::make('w1', 1));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    $this->artisan('sync:reconcile')->assertExitCode(0);
});

it('lets --transport win over the configured reconcile transport', function (): void {
    config()->set('platform-sync.reconcile.transport', 'sqs');

    $this->transport->push(Envelopes::make('w1', 1));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    $this->artisan('sync:reconcile --transport=array')->assertExitCode(0);
});

it('ships sqs as the default transport, per ADR 0002', function (): void {
    // `poll` đứng ở đây trước khi ai đọc ADR 0002 — không phải vì một phép đo
    // nào. Bài này là cái ngăn nó lặng lẽ quay lại.
    $shipped = require __DIR__.'/../../config/platform-sync.php';

    expect($shipped['default'])->toBe('sqs')
        ->and($shipped['transports'])->toHaveKeys(['sqs', 'poll']);
});

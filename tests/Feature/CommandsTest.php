<?php

declare(strict_types=1);

use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\TransportManager;

beforeEach(function (): void {
    FakeProjector::reset();
    $this->transport = new ArrayTransport;
    app(TransportManager::class)->set('array', $this->transport);
});

function registerWidget(ProjectionMode $mode = ProjectionMode::Live): void
{
    app(SyncRegistry::class)
        ->resource(Envelopes::TYPE)
        ->projector(FakeProjector::class)
        ->requires(['id', 'name'])
        ->mode($mode);
}

it('fails sync:pull when nothing has a projector, rather than reporting success', function (): void {
    // Thoát 0 ở đây dựng nên một hệ trông như đang chạy mà không tài nguyên nào
    // tới đích — dạng hỏng đắt nhất, vì nó không sinh ra lỗi nào để ai đó thấy.
    $this->artisan('sync:pull')
        ->expectsOutputToContain('No resource type has a projector registered')
        ->assertExitCode(1);
});

it('runs sync:pull green when everything applies', function (): void {
    registerWidget();
    $this->transport->push(Envelopes::make('w1', 1));

    $this->artisan('sync:pull')->assertExitCode(0);

    expect(FakeProjector::$state)->toHaveKey('w1');
});

it('turns sync:pull red when an envelope is rejected', function (): void {
    registerWidget();
    $this->transport->push(Envelopes::make('w1', 1, ['id' => 'w1']));

    $this->artisan('sync:pull')->assertExitCode(1);
});

it('exits sync:reconcile with 2 when there is drift, so a scheduler can tell', function (): void {
    registerWidget();
    $this->transport->push(Envelopes::make('w1', 1));

    $this->artisan('sync:reconcile')->assertExitCode(2);
});

it('exits sync:reconcile with 0 when both sides agree', function (): void {
    registerWidget();
    $this->transport->push(Envelopes::make('w1', 1));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    $this->artisan('sync:reconcile')->assertExitCode(0);
});

it('says out loud when --repair was ignored because the type is in shadow', function (): void {
    // Người vận hành vừa gõ --repair và sẽ tin rằng nó đã chạy.
    registerWidget(ProjectionMode::Shadow);
    $this->transport->push(Envelopes::make('w1', 1));

    $this->artisan('sync:reconcile --repair')
        ->expectsOutputToContain('--repair ignored')
        ->assertExitCode(2);

    expect(FakeProjector::$state)->toBe([]);
});

it('warns in sync:status about types that will never sync', function (): void {
    $this->artisan('sync:status')
        ->expectsOutputToContain('No projector registered')
        ->assertExitCode(0);
});

<?php

declare(strict_types=1);

use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Projection\Reconciler;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\TransportManager;
use Illuminate\Support\Facades\DB;

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

it('reports no drift when both sides agree', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($result->inSync())->toBeTrue()
        ->and($result->remoteCount)->toBe(1)
        ->and($result->localCount)->toBe(1);
});

it('finds a resource Platform has and the consumer does not', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($result->drift)->toBe(['missing_local' => 1]);
});

it('finds a field that disagrees and names it', function (): void {
    $this->transport->push(Envelopes::make('w1', 1, ['id' => 'w1', 'name' => 'Platform name']));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Local name'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($result->drift)->toBe(['field_mismatch' => 1])
        ->and(json_decode((string) DB::table('platform_sync_drift')->value('differing_fields'), true))->toBe(['name']);
});

it('finds a resource the consumer still holds after Platform dropped it', function (): void {
    // Đây là loại lệch mà LUỒNG EVENT không bao giờ thấy: nếu chính event xoá
    // là cái bị mất thì không có bản nào tới sau để lộ ra.
    FakeProjector::$state['ghost'] = ['id' => 'ghost', 'name' => 'gone upstream'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($result->drift)->toBe(['orphan_local' => 1]);
});

it('ignores local-only fields instead of calling every resource drifted', function (): void {
    // Consumer gần như luôn giữ thêm cột riêng. Đòi hai mảng bằng nhau tuyệt
    // đối thì báo cáo luôn đỏ, và một báo cáo luôn đỏ thì không ai đọc.
    $this->transport->push(Envelopes::make('w1', 1, ['id' => 'w1', 'name' => 'Widget w1']));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1', 'local_pk' => 991, 'synced_at' => 'now'];

    expect(app(Reconciler::class)->reconcile(Envelopes::TYPE)->inSync())->toBeTrue();
});

it('does not treat driver type wobble as drift', function (): void {
    $this->transport->push(Envelopes::make('w1', 1, ['id' => 'w1', 'name' => 'x', 'active' => true, 'seats' => 12]));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'x', 'active' => 1, 'seats' => '12'];

    expect(app(Reconciler::class)->reconcile(Envelopes::TYPE)->inSync())->toBeTrue();
});

it('is read-only unless repair is asked for', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));

    app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect(FakeProjector::$state)->toBe([]);
});

it('repairs when asked and the type is live', function (): void {
    $this->transport->push(Envelopes::make('w1', 7));

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE, repair: true);

    expect($result->repaired)->toBe(1)
        ->and(FakeProjector::$state)->toHaveKey('w1')
        // Sửa xong phải đẩy vị trí, nếu không event kế của Platform sẽ bị coi
        // là mới trong khi trạng thái đã bằng nhau — và tệ hơn, một event CŨ
        // hơn sẽ áp được.
        ->and(app(InboxStore::class)->appliedSequence(Envelopes::TYPE, 'w1'))->toBe(7);
});

it('refuses to repair a type that is still in shadow, and says why', function (): void {
    app(SyncRegistry::class)->resource(Envelopes::TYPE)->mode(ProjectionMode::Shadow);
    $this->transport->push(Envelopes::make('w1', 1));

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE, repair: true);

    expect($result->repaired)->toBe(0)
        ->and($result->repairAllowed)->toBeFalse()
        ->and($result->repairBlockedReason)->toContain('shadow mode')
        ->and(FakeProjector::$state)->toBe([]);
});

it('keeps each run separate so two reports never blend', function (): void {
    $this->transport->push(Envelopes::make('w1', 1));

    $first = app(Reconciler::class)->reconcile(Envelopes::TYPE);
    $second = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($first->runId)->not->toBe($second->runId)
        ->and($first->drift)->toBe(['missing_local' => 1])
        ->and($second->drift)->toBe(['missing_local' => 1])
        ->and(DB::table('platform_sync_drift')->count())->toBe(2);
});

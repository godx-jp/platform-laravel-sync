<?php

declare(strict_types=1);

use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Projection\Reconciler;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\EndlessSnapshotTransport;
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

// ─── Trần số trang ─────────────────────────────────────────────────────────

it('stops after a page cap instead of looping forever on a feed that never ends', function (): void {
    // `FeedPuller` có `maxPages`; đối soát thì không, và nó chạy đúng cái vòng
    // `do { } while ($page->hasMore)`. Platform nói `has_more: true` mãi mãi —
    // vì lỗi phân trang của họ, hoặc vì feed sinh nhanh hơn tốc độ đọc — là đủ
    // để treo tiến trình của consumer, không lỗi, không dấu vết.
    $endless = new EndlessSnapshotTransport;
    app(TransportManager::class)->set('array', $endless);

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE, maxPages: 3);

    expect($endless->calls)->toBe(3)
        ->and($result->remoteCount)->toBe(3);
});

it('says the reconciliation is incomplete when it hit the cap', function (): void {
    // Một báo cáo lệch dựng trên NỬA dữ liệu mà trông như đầy đủ tệ hơn không
    // có báo cáo: người đọc sẽ hành động theo nó.
    //
    // Hai trang đọc được KHỚP hoàn toàn với cục bộ, có chủ đích: đó là ca duy
    // nhất mà `inSync()` load-bearing. Nếu để lượt này có lệch thì `inSync()`
    // trả false vì lệch chứ không vì dở dang, và vế `complete` của nó thành
    // rào rỗng — đo được: gỡ vế ấy ra mà bài vẫn xanh.
    app(TransportManager::class)->set('array', new EndlessSnapshotTransport);
    FakeProjector::$state['remote_1'] = ['id' => 'remote_1', 'name' => 'Widget 1'];
    FakeProjector::$state['remote_2'] = ['id' => 'remote_2', 'name' => 'Widget 2'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE, maxPages: 2);

    expect($result->drift)->toBe([])
        ->and($result->complete)->toBeFalse()
        ->and($result->incompleteReason)->toContain('2')
        // "Không thấy lệch trên nửa ảnh chụp" KHÔNG phải "hai bên bằng nhau".
        ->and($result->inSync())->toBeFalse();
});

it('does not call every local row an orphan just because the snapshot was cut short', function (): void {
    // Chiều `orphan_local` đo bằng phép TRỪ TẬP HỢP: "cục bộ có, ảnh chụp
    // không". Ảnh chụp bị cắt giữa chừng thì phép trừ đó báo nhầm HÀNG LOẠT —
    // và `--repair` ở lượt sau sẽ xoá theo.
    app(TransportManager::class)->set('array', new EndlessSnapshotTransport);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];
    FakeProjector::$state['w2'] = ['id' => 'w2', 'name' => 'Widget w2'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE, maxPages: 2);

    expect($result->drift)->not->toHaveKey('orphan_local')
        // Đếm thì vẫn đếm — con số cục bộ là thật, chỉ có KẾT LUẬN về nó là
        // thứ không rút ra được từ nửa ảnh chụp.
        ->and($result->localCount)->toBe(2);
});

it('still finds orphans when the snapshot finished', function (): void {
    // Rào cho bản vá ở trên: chặn "chiều ngược" không được phép tắt luôn.
    FakeProjector::$state['ghost'] = ['id' => 'ghost', 'name' => 'gone upstream'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($result->complete)->toBeTrue()
        ->and($result->drift)->toBe(['orphan_local' => 1]);
});

// ─── Trường thừa của Platform ──────────────────────────────────────────────

it('does not call a resource drifted because Platform sent a field the consumer does not mirror', function (): void {
    // Platform thêm một cột thì MỌI tài nguyên loại đó thành `field_mismatch`
    // vĩnh viễn. Báo cáo luôn đỏ = không ai đọc = mất luôn công cụ.
    $this->transport->push(Envelopes::make('w1', 1, [
        'id' => 'w1',
        'name' => 'Widget w1',
        'billing_plan' => 'enterprise',
        'feature_flags' => ['beta' => true],
    ]));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    expect(app(Reconciler::class)->reconcile(Envelopes::TYPE)->inSync())->toBeTrue();
});

it('still names a mirrored field whose value really disagrees', function (): void {
    // Chiều ngược của bài trên. Bỏ qua trường Platform thừa KHÔNG được phép
    // trượt thành bỏ qua mọi so sánh — đó là cách một rào trở thành rào rỗng.
    $this->transport->push(Envelopes::make('w1', 1, [
        'id' => 'w1',
        'name' => 'Platform name',
        'billing_plan' => 'enterprise',
    ]));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Local name'];

    $result = app(Reconciler::class)->reconcile(Envelopes::TYPE);

    expect($result->drift)->toBe(['field_mismatch' => 1])
        ->and(json_decode((string) DB::table('platform_sync_drift')->value('differing_fields'), true))
        ->toBe(['name']);
});

it('reports drift when the projector shares no field at all with the payload', function (): void {
    // Không giao nhau MỘT trường nào nghĩa là `current()` trả lược đồ sai —
    // hợp đồng đòi "cùng khoá, cùng kiểu". Im lặng ở đây sẽ tuyên bố đồng bộ
    // cho một projector chưa bao giờ so được gì.
    $this->transport->push(Envelopes::make('w1', 1, ['id' => 'w1', 'name' => 'Widget w1']));
    FakeProjector::$state['w1'] = ['branch_uuid' => 'w1', 'display_name' => 'Widget w1'];

    expect(app(Reconciler::class)->reconcile(Envelopes::TYPE)->drift)->toBe(['field_mismatch' => 1]);
});

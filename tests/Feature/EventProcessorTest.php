<?php

declare(strict_types=1);

use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Inbox\Verdict;
use Godx\Sync\Projection\EventProcessor;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Illuminate\Support\Facades\DB;

beforeEach(function (): void {
    FakeProjector::reset();

    app(SyncRegistry::class)
        ->resource(Envelopes::TYPE)
        ->projector(FakeProjector::class)
        ->requires(['id', 'name'])
        ->mode(ProjectionMode::Live);
});

function process($event): Verdict
{
    return app(EventProcessor::class)->process($event);
}

it('applies a first event and advances the position', function (): void {
    $event = Envelopes::make('w1', 10);

    expect(process($event))->toBe(Verdict::Applied)
        ->and(FakeProjector::$state['w1']['name'])->toBe('Widget w1')
        ->and(app(InboxStore::class)->appliedSequence(Envelopes::TYPE, 'w1'))->toBe(10);
});

it('drops a repeat of the same event id without calling the projector twice', function (): void {
    $event = Envelopes::make('w1', 10);

    process($event);
    $applied = count(FakeProjector::$applied);

    expect(process($event))->toBe(Verdict::Duplicate)
        ->and(FakeProjector::$applied)->toHaveCount($applied);
});

it('refuses an event whose sequence does not move forward', function (): void {
    process(Envelopes::make('w1', 10, ['id' => 'w1', 'name' => 'new']));

    $late = Envelopes::make('w1', 9, ['id' => 'w1', 'name' => 'OLD VALUE']);

    expect(process($late))->toBe(Verdict::Stale)
        // Đây là toàn bộ lý do `sequence` tồn tại: một event đến muộn KHÔNG
        // được thắng chỉ vì nó tới sau.
        ->and(FakeProjector::$state['w1']['name'])->toBe('new');
});

it('refuses an event that merely repeats the applied sequence', function (): void {
    process(Envelopes::make('w1', 10));

    expect(process(Envelopes::make('w1', 10, ['id' => 'w1', 'name' => 'rewrite'])))->toBe(Verdict::Stale);
});

it('applies across a sequence gap and records it, instead of blocking the queue', function (): void {
    process(Envelopes::make('w1', 10));

    // Nói rằng nó nối sau 15, nhưng cục bộ mới ở 10 — thiếu một đoạn.
    $event = Envelopes::make('w1', 16, ['id' => 'w1', 'name' => 'later'], previousSequence: 15);

    expect(process($event))->toBe(Verdict::GapNoted)
        ->and(FakeProjector::$state['w1']['name'])->toBe('later');

    $note = DB::table('platform_sync_inbox')->where('event_id', $event->id)->value('note');
    expect($note)->toContain('Sequence gap')->toContain('reconcile');
});

it('does not call a gap when prevsequence lines up with the applied position', function (): void {
    process(Envelopes::make('w1', 10));

    expect(process(Envelopes::make('w1', 11, previousSequence: 10)))->toBe(Verdict::Applied);
});

it('rejects a payload missing a required field before the projector sees it', function (): void {
    $event = Envelopes::make('w1', 10, ['id' => 'w1']);

    expect(process($event))->toBe(Verdict::Rejected)
        ->and(FakeProjector::$state)->toBe([])
        // Vị trí KHÔNG được tiến: bản sửa gửi sau phải còn áp được.
        ->and(app(InboxStore::class)->appliedSequence(Envelopes::TYPE, 'w1'))->toBeNull();
});

it('lets a delete through even though it carries no full body', function (): void {
    process(Envelopes::make('w1', 10));

    expect(process(Envelopes::make('w1', 11, ['id' => 'w1'], verb: 'deleted')))->toBe(Verdict::Applied)
        ->and(FakeProjector::$state)->not->toHaveKey('w1');
});

it('records a projector failure and leaves the position untouched', function (): void {
    FakeProjector::$throwOnApply = true;
    $event = Envelopes::make('w1', 10);

    expect(process($event))->toBe(Verdict::Failed)
        ->and(app(InboxStore::class)->appliedSequence(Envelopes::TYPE, 'w1'))->toBeNull();

    $row = DB::table('platform_sync_inbox')->where('event_id', $event->id)->first();
    expect($row->note)->toContain('projector exploded')
        // Chưa xử lý xong thì chưa được coi là đã yên: `sync:status` phải nêu nó.
        ->and($row->settled_at)->toBeNull();
});

it('writes nothing in shadow mode but still records what would have changed', function (): void {
    app(SyncRegistry::class)->resource(Envelopes::TYPE)->mode(ProjectionMode::Shadow);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'local value'];

    expect(process(Envelopes::make('w1', 10, ['id' => 'w1', 'name' => 'platform value'])))->toBe(Verdict::Shadowed)
        ->and(FakeProjector::$state['w1']['name'])->toBe('local value');

    $drift = DB::table('platform_sync_drift')->first();
    expect($drift->kind)->toBe('field_mismatch')
        ->and(json_decode((string) $drift->differing_fields, true))->toBe(['name']);
});

it('does not advance the position in shadow mode', function (): void {
    // Nếu shadow đẩy vị trí, thì lần bật live đầu tiên sẽ coi MỌI event đã chạy
    // shadow là cũ và bỏ qua — bảng thật đứng im trong khi mọi con số nói rằng
    // đồng bộ đang chạy.
    app(SyncRegistry::class)->resource(Envelopes::TYPE)->mode(ProjectionMode::Shadow);

    process(Envelopes::make('w1', 10));

    expect(app(InboxStore::class)->appliedSequence(Envelopes::TYPE, 'w1'))->toBeNull();

    app(SyncRegistry::class)->resource(Envelopes::TYPE)->mode(ProjectionMode::Live);

    expect(process(Envelopes::make('w1', 10)))->toBe(Verdict::Applied)
        ->and(FakeProjector::$state)->toHaveKey('w1');
});

it('records no drift in shadow mode when local already matches', function (): void {
    app(SyncRegistry::class)->resource(Envelopes::TYPE)->mode(ProjectionMode::Shadow);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'name' => 'Widget w1'];

    process(Envelopes::make('w1', 10));

    expect(DB::table('platform_sync_drift')->count())->toBe(0);
});

it('rejects a type that has no projector rather than silently skipping it', function (): void {
    app(SyncRegistry::class)->resource('godx.test.orphan')->requires([]);

    $event = Envelopes::make('o1', 1);
    $event = new CloudEvent(
        id: $event->id, source: $event->source, type: 'godx.test.orphan.updated',
        subject: 'godx.test.orphan/o1', time: $event->time, data: ['id' => 'o1'],
        sequence: 1, tenantId: 'org_1',
    );

    expect(process($event))->toBe(Verdict::Rejected);
});

// ─── subject id ↔ data.id ──────────────────────────────────────────────────

it('refuses an envelope whose subject and data.id name two different resources', function (): void {
    // Sổ nhận, `applied_sequence` và báo cáo lệch đều khoá theo `resourceId()`
    // cắt từ `subject`; projector của consumer thì gần như luôn ghi theo
    // `data['id']`. Hai giá trị lệch nhau ⇒ phép chống-ghi-đè canh MỘT tài
    // nguyên trong khi phép ghi rơi vào tài nguyên KHÁC. Không tầng nào ném
    // lỗi, và cả hai hàng đều trông bình thường sau đó.
    $event = Envelopes::make('w1', 10, ['id' => 'w2', 'name' => 'Widget w2']);

    expect(process($event))->toBe(Verdict::Rejected)
        ->and(FakeProjector::$state)->toBe([])
        ->and(app(InboxStore::class)->appliedSequence(Envelopes::TYPE, 'w1'))->toBeNull();

    $note = (string) DB::table('platform_sync_inbox')->where('event_id', $event->id)->value('note');
    expect($note)->toContain('w1')->toContain('w2');
});

it('leaves a resource type that carries no id key alone', function (): void {
    // Không phải mọi loại đều đặt danh tính vào `data`. Bắt vạ lây ở đây là
    // biến một rào đúng thành một cổng chặn dữ liệu hợp lệ.
    app(SyncRegistry::class)->resource(Envelopes::TYPE)->requires(['name']);

    expect(process(Envelopes::make('w1', 10, ['name' => 'Widget without an id key'])))
        ->toBe(Verdict::Applied);
});

it('accepts the ordinary case where subject and data.id agree', function (): void {
    expect(process(Envelopes::make('w1', 10, ['id' => 'w1', 'name' => 'Widget w1'])))
        ->toBe(Verdict::Applied);
});

it('checks identity even on a delete, which still names what it deletes', function (): void {
    expect(process(Envelopes::make('w1', 10, ['id' => 'w9'], verb: 'deleted')))
        ->toBe(Verdict::Rejected);
});

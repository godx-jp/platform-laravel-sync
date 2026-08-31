<?php

declare(strict_types=1);

use Godx\Sync\Projection\EventProcessor;
use Godx\Sync\Projection\Reconciler;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\Envelopes;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\TransportManager;
use Illuminate\Support\Facades\DB;

/**
 * `->unordered([...])` — trường mà THỨ TỰ không mang nghĩa.
 *
 * Bài đo được của cả cụm này: consumer sắp xếp một danh sách để đọc cho dễ,
 * Platform trả lại đúng thứ tự nó nhận, và phép so nhạy thứ tự biến MỌI tài
 * nguyên loại đó thành `field_mismatch` vĩnh viễn. Một báo cáo lệch luôn đỏ thì
 * không ai đọc — tức là mất luôn công cụ, và mất đúng lúc nó cần nhất.
 */
beforeEach(function (): void {
    FakeProjector::reset();

    app(SyncRegistry::class)
        ->resource(Envelopes::TYPE)
        ->projector(FakeProjector::class)
        ->requires(['id'])
        ->mode(ProjectionMode::Shadow);

    $this->transport = new ArrayTransport;
    app(TransportManager::class)->set('array', $this->transport);
});

/** @param  list<string>  $fields */
function declareUnordered(array $fields): void
{
    app(SyncRegistry::class)->resource(Envelopes::TYPE)->unordered($fields);
}

/** @param  array<string, mixed>  $remote */
function shadowCompare(array $remote): void
{
    app(EventProcessor::class)->process(Envelopes::make('w1', 10, $remote));
}

/** @return list<string> */
function driftedFields(): array
{
    $row = DB::table('platform_sync_drift')->first();

    return $row === null ? [] : (array) json_decode((string) $row->differing_fields, true);
}

it('calls the same set in a different order no drift at all', function (): void {
    declareUnordered(['permissions']);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'permissions' => ['a.view', 'b.view', 'c.view']];

    shadowCompare(['id' => 'w1', 'permissions' => ['c.view', 'a.view', 'b.view']]);

    expect(DB::table('platform_sync_drift')->count())->toBe(0);
});

it('still calls a different set drift, and names the field', function (): void {
    // Nửa kia của phép đo. Bỏ thứ tự KHÔNG được biến thành bỏ nhìn: một
    // permission thừa hay thiếu vẫn phải hiện ra.
    declareUnordered(['permissions']);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'permissions' => ['a.view', 'b.view']];

    shadowCompare(['id' => 'w1', 'permissions' => ['b.view', 'a.view', 'c.view']]);

    expect(driftedFields())->toBe(['permissions']);
});

it('keeps a repeated element visible instead of collapsing it away', function (): void {
    // Bỏ THỨ TỰ, giữ SỐ LẦN. Một phép so "tập" theo nghĩa toán học sẽ nuốt một
    // phần tử bị lặp — mà lặp là dữ liệu hỏng, không phải cách liệt kê khác.
    declareUnordered(['permissions']);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'permissions' => ['a.view', 'b.view']];

    shadowCompare(['id' => 'w1', 'permissions' => ['a.view', 'a.view', 'b.view']]);

    expect(driftedFields())->toBe(['permissions']);
});

it('leaves every field NOT declared unordered sensitive to order', function (): void {
    // Mặc định không đổi, và đây là chỗ chứng minh. Thứ tự CÓ nghĩa ở nhiều
    // trường (thứ tự hiển thị, thứ tự ưu tiên, một chuỗi sự kiện); đổi mặc định
    // là làm hệ mù với cả một lớp lệch.
    declareUnordered(['permissions']);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'steps' => ['one', 'two']];

    shadowCompare(['id' => 'w1', 'steps' => ['two', 'one']]);

    expect(driftedFields())->toBe(['steps']);
});

it('falls back to the ordinary comparison when the field is not an array', function (): void {
    // Khai `unordered` cho một trường vô hướng không có nghĩa gì — và nó không
    // được âm thầm biến thành "bằng nhau".
    declareUnordered(['permissions']);
    FakeProjector::$state['w1'] = ['id' => 'w1', 'permissions' => 'a.view'];

    shadowCompare(['id' => 'w1', 'permissions' => 'b.view']);

    expect(driftedFields())->toBe(['permissions']);
});

it('honours the declaration on the reconcile path too, not only the event path', function (): void {
    // Hai đường vào `DriftRecorder` là hai lời gọi riêng (`compareOne` và
    // `compareSnapshotRow`). Sửa một chỗ và quên chỗ kia thì `sync:pull` im
    // lặng còn `sync:reconcile` đỏ vĩnh viễn — đúng lệnh mà người vận hành
    // dùng để trả lời "tôi có đang thiếu gì không".
    declareUnordered(['permissions']);
    $this->transport->push(Envelopes::make('w1', 1, ['id' => 'w1', 'permissions' => ['c', 'a', 'b']]));
    FakeProjector::$state['w1'] = ['id' => 'w1', 'permissions' => ['a', 'b', 'c']];

    expect(app(Reconciler::class)->reconcile(Envelopes::TYPE)->inSync())->toBeTrue();
});

it('defaults to declaring nothing unordered', function (): void {
    expect(app(SyncRegistry::class)->resource('godx.test.undeclared')->unorderedFields())->toBe([]);
});

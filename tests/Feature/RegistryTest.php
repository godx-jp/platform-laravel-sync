<?php

declare(strict_types=1);

use Godx\Sync\Directory\DirectoryResources;
use Godx\Sync\Exceptions\UnknownResourceType;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\FakeProjector;

it('starts every resource in shadow', function (): void {
    // Shadow là mặc định vì projector ghi vào bảng đang phục vụ khách, và một
    // projector sai KHÔNG ném lỗi — nó ghi sai rồi mọi phép kiểm vẫn xanh.
    expect(app(SyncRegistry::class)->resource('godx.test.fresh')->projectionMode())
        ->toBe(ProjectionMode::Shadow);
});

it('lets configuration raise a type to live', function (): void {
    $registry = app(SyncRegistry::class);
    $registry->resource('godx.test.thing');
    $registry->applyModes(['godx.test.thing' => 'live']);

    expect($registry->definition('godx.test.thing')->projectionMode())->toBe(ProjectionMode::Live);
});

it('leaves a mis-spelled mode in shadow instead of guessing', function (): void {
    $registry = app(SyncRegistry::class);
    $registry->resource('godx.test.thing')->mode(ProjectionMode::Shadow);
    $registry->applyModes(['godx.test.thing' => 'LIVE!']);

    expect($registry->definition('godx.test.thing')->projectionMode())->toBe(ProjectionMode::Shadow);
});

it('ships the Platform vocabulary already registered', function (): void {
    expect(app(SyncRegistry::class)->types())->toContain(
        DirectoryResources::BRANCH,
        DirectoryResources::ORGANIZATION,
        DirectoryResources::ROLE_BINDING,
    );
});

it('ships no projector for any Platform type', function (): void {
    // Package CỐ Ý không đoán lược đồ của consumer. Ship một projector "dùng
    // chung" nghĩa là ghi đè bảng của người khác theo phỏng đoán.
    $registry = app(SyncRegistry::class);

    foreach (DirectoryResources::all() as $type) {
        expect($registry->definition($type)->projectorClass())->toBeNull();
    }

    expect($registry->projectableTypes())->toBe([]);
});

it('requires branch_id to be PRESENT on a role binding, even when null', function (): void {
    // `branch_id = null` nghĩa là MỌI chi nhánh, không phải "không chi nhánh
    // nào". Khoá vắng mặt và khoá mang null là hai chuyện khác nhau, và gộp
    // chúng biến một binding toàn tổ chức thành binding không phạm vi.
    expect(app(SyncRegistry::class)->definition(DirectoryResources::ROLE_BINDING)->required())
        ->toContain('branch_id');
});

it('names the known types when asked for one that is not registered', function (): void {
    app(SyncRegistry::class)->definition('godx.test.nope');
})->throws(UnknownResourceType::class, 'Known types:');

it('lists only types that actually have a projector as projectable', function (): void {
    $registry = app(SyncRegistry::class);
    $registry->resource('godx.test.with')->projector(FakeProjector::class);
    $registry->resource('godx.test.without');

    expect($registry->projectableTypes())->toBe(['godx.test.with']);
});

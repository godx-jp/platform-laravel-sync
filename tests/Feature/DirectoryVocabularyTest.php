<?php

declare(strict_types=1);

use Godx\Sync\Directory\DirectoryResources;
use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Inbox\Verdict;
use Godx\Sync\Projection\EventProcessor;
use Godx\Sync\Registry\ProjectionMode;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Tests\Fixtures\FakeProjector;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

beforeEach(function (): void {
    FakeProjector::reset();

    app(SyncRegistry::class)
        ->resource(DirectoryResources::ORGANIZATION)
        ->projector(FakeProjector::class)
        ->mode(ProjectionMode::Live);
});

/** @param  array<string, mixed>  $data */
function organizationEvent(array $data, string $verb = 'created'): CloudEvent
{
    return new CloudEvent(
        id: (string) Str::ulid(),
        source: 'https://id.godx.jp',
        type: DirectoryResources::ORGANIZATION.'.'.$verb,
        subject: DirectoryResources::ORGANIZATION.'/'.($data['id'] ?? 'org_1'),
        time: new DateTimeImmutable('2026-08-31T00:00:00Z'),
        data: $data,
        sequence: 1,
        tenantId: 'org_1',
    );
}

it('never asks for a field called status anywhere in the directory vocabulary', function (): void {
    // Không một payload nào của Platform mang `status`; cờ hoạt động là
    // `is_active` (boolean) ở CẢ HAI phía. Một trường bắt buộc mà bên phát
    // không bao giờ gửi thì mọi envelope của loại đó bị từ chối, mãi mãi.
    foreach (DirectoryResources::all() as $type) {
        expect(DirectoryResources::requiredFor($type))->not->toContain('status');
    }
});

it('requires slug on every type whose local column is NOT NULL', function (): void {
    // `organizations.slug` · `brands.slug` · `branches.slug` đều NOT NULL và
    // không có default. Thiếu nó ở đây thì envelope qua cửa registry rồi đổ ở
    // tầng SQL thành `Verdict::Failed` — cùng một sự cố, nhưng báo cáo ở chỗ
    // khó đọc nhất và mang một thông điệp của driver DB.
    expect(DirectoryResources::requiredFor(DirectoryResources::ORGANIZATION))->toContain('slug')
        ->and(DirectoryResources::requiredFor(DirectoryResources::BRAND))->toContain('slug')
        ->and(DirectoryResources::requiredFor(DirectoryResources::BRANCH))->toContain('slug');
});

it('accepts an organization payload that carries is_active and no status', function (): void {
    $verdict = app(EventProcessor::class)->process(organizationEvent([
        'id' => 'org_1',
        'name' => 'Betoya',
        'slug' => 'betoya',
        'is_active' => true,
    ]));

    expect($verdict)->toBe(Verdict::Applied)
        ->and(FakeProjector::$state)->toHaveKey('org_1');
});

it('rejects an organization created without a slug, and says so in words', function (): void {
    $event = organizationEvent(['id' => 'org_1', 'name' => 'Betoya', 'is_active' => true]);

    expect(app(EventProcessor::class)->process($event))->toBe(Verdict::Rejected)
        ->and(FakeProjector::$state)->toBe([]);

    expect((string) DB::table('platform_sync_inbox')->where('event_id', $event->id)->value('note'))
        ->toContain('slug');
});

<?php

declare(strict_types=1);

namespace Godx\Sync\Tests\Fixtures;

use Godx\Sync\Contracts\Projector;
use Godx\Sync\Envelope\CloudEvent;
use RuntimeException;

/**
 * Projector giả, giữ trạng thái trong một kho tĩnh.
 *
 * Tĩnh vì container dựng lại projector cho mỗi lần `make()` — một thuộc tính
 * thường sẽ mất giữa hai envelope, và bài test sẽ đo nhầm "projector không ghi
 * gì" trong khi thứ nó đo là vòng đời của container.
 */
final class FakeProjector implements Projector
{
    /** @var array<string, array<string, mixed>> */
    public static array $state = [];

    public static bool $throwOnApply = false;

    /** @var list<string> */
    public static array $applied = [];

    public static function reset(): void
    {
        self::$state = [];
        self::$applied = [];
        self::$throwOnApply = false;
    }

    public function current(string $resourceId): ?array
    {
        return self::$state[$resourceId] ?? null;
    }

    public function apply(CloudEvent $event): void
    {
        if (self::$throwOnApply) {
            throw new RuntimeException('projector exploded');
        }

        self::$applied[] = $event->id;

        if ($event->verb() === 'deleted') {
            unset(self::$state[$event->resourceId()]);

            return;
        }

        self::$state[$event->resourceId()] = $event->data;
    }

    public function localIds(): iterable
    {
        return array_keys(self::$state);
    }
}

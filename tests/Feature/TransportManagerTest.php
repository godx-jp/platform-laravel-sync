<?php

declare(strict_types=1);

use Godx\Sync\Contracts\Transport;
use Godx\Sync\Exceptions\UnknownTransport;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\PollTransport;
use Godx\Sync\Transport\TransportManager;

it('resolves the configured default', function (): void {
    expect(app(TransportManager::class)->transport())->toBeInstanceOf(ArrayTransport::class);
});

it('resolves the poll driver', function (): void {
    expect(app(TransportManager::class)->transport('poll'))->toBeInstanceOf(PollTransport::class);
});

it('keys the cache by CONFIG name, not by driver', function (): void {
    // Hai transport `poll` trỏ về hai Platform khác nhau là cấu hình bình
    // thường (staging và production). Cache theo tên driver sẽ trả nhầm cái
    // thứ nhất cho cả hai — im lặng, và với endpoint sai.
    config()->set('platform-sync.transports.poll_staging', ['driver' => 'poll', 'endpoint' => 'https://staging.example/sync']);
    app()->forgetInstance(TransportManager::class);

    $manager = app(TransportManager::class);

    expect($manager->transport('poll'))->not->toBe($manager->transport('poll_staging'));
});

it('names a transport that is not configured', function (): void {
    app(TransportManager::class)->transport('ghost');
})->throws(UnknownTransport::class, 'is not configured');

it('names the driver a configured transport asks for but nobody registered', function (): void {
    config()->set('platform-sync.transports.mq', ['driver' => 'rabbitmq']);
    app()->forgetInstance(TransportManager::class);

    app(TransportManager::class)->transport('mq');
})->throws(UnknownTransport::class, 'PlatformSync::extend');

it('lets an application register its own driver without touching this package', function (): void {
    config()->set('platform-sync.transports.mq', ['driver' => 'rabbitmq']);
    app()->forgetInstance(TransportManager::class);

    $manager = app(TransportManager::class);
    $manager->extend('rabbitmq', fn (): Transport => new class implements Transport
    {
        public function name(): string
        {
            return 'rabbitmq';
        }
    });

    expect($manager->transport('mq')->name())->toBe('rabbitmq');
});

<?php

declare(strict_types=1);

use Godx\Sync\Contracts\Transport;
use Godx\Sync\Exceptions\UnknownTransport;
use Godx\Sync\Transport\ArrayTransport;
use Godx\Sync\Transport\PollTransport;
use Godx\Sync\Transport\SqsTransport;
use Godx\Sync\Transport\TransportManager;

it('resolves the configured default', function (): void {
    expect(app(TransportManager::class)->transport())->toBeInstanceOf(ArrayTransport::class);
});

it('resolves the poll driver', function (): void {
    expect(app(TransportManager::class)->transport('poll'))->toBeInstanceOf(PollTransport::class);
});

it('resolves the sqs driver without reaching for the network', function (): void {
    // Dựng client KHÔNG được phân giải credential hay gọi metadata service:
    // `TransportManager` dựng mọi transport để trả lời câu "cái nào chụp được",
    // và một lượt gọi mạng ở đó biến một câu hỏi cấu hình thành một lần treo.
    config()->set('platform-sync.transports.sqs', [
        'driver' => 'sqs',
        'queue_url' => 'https://sqs/tempo-identity-events',
        'region' => 'ap-northeast-1',
    ]);

    expect(app(TransportManager::class)->transport('sqs'))->toBeInstanceOf(SqsTransport::class);
});

it('keys the cache by CONFIG name, not by driver', function (): void {
    // Hai transport `poll` trỏ về hai Platform khác nhau là cấu hình bình
    // thường (staging và production). Cache theo tên driver sẽ trả nhầm cái
    // thứ nhất cho cả hai — im lặng, và với endpoint sai.
    config()->set('platform-sync.transports.poll_staging', ['driver' => 'poll', 'endpoint' => 'https://staging.example/sync']);

    $manager = app(TransportManager::class);

    expect($manager->transport('poll'))->not->toBe($manager->transport('poll_staging'));
});

it('names a transport that is not configured', function (): void {
    app(TransportManager::class)->transport('ghost');
})->throws(UnknownTransport::class, 'is not configured');

it('names the driver a configured transport asks for but nobody registered', function (): void {
    config()->set('platform-sync.transports.mq', ['driver' => 'rabbitmq']);

    app(TransportManager::class)->transport('mq');
})->throws(UnknownTransport::class, 'PlatformSync::extend');

it('lets an application register its own driver without touching this package', function (): void {
    config()->set('platform-sync.transports.mq', ['driver' => 'rabbitmq']);

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

it('sees a configuration change made after the container already resolved it', function (): void {
    // Chụp config thành mảng lúc dựng thì `config()->set()` sau đó không có tác
    // dụng nào, và triệu chứng KHÔNG phải lỗi cấu hình — nó là một lượt gọi
    // mạng thật tới transport cũ.
    $manager = app(TransportManager::class);
    expect($manager->defaultName())->toBe('array');

    config()->set('platform-sync.default', 'poll');

    expect($manager->defaultName())->toBe('poll')
        ->and($manager->transport())->toBeInstanceOf(PollTransport::class);
});

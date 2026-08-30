<?php

declare(strict_types=1);

namespace Godx\Sync;

use Godx\Sync\Console\PullCommand;
use Godx\Sync\Console\ReconcileCommand;
use Godx\Sync\Console\StatusCommand;
use Godx\Sync\Directory\DirectoryResources;
use Godx\Sync\Inbox\CursorStore;
use Godx\Sync\Inbox\InboxStore;
use Godx\Sync\Projection\DriftRecorder;
use Godx\Sync\Projection\EventProcessor;
use Godx\Sync\Projection\FeedPuller;
use Godx\Sync\Projection\Reconciler;
use Godx\Sync\Registry\SyncRegistry;
use Godx\Sync\Transport\TransportManager;
use Illuminate\Contracts\Config\Repository as Config;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\DatabaseManager;
use Illuminate\Support\ServiceProvider;

final class PlatformSyncServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/platform-sync.php', 'platform-sync');

        $this->app->singleton(SyncRegistry::class, function (): SyncRegistry {
            $registry = new SyncRegistry;

            // Từ vựng của Platform luôn có mặt. Projector thì KHÔNG — ứng dụng
            // tự khai, và loại nào chưa khai sẽ bị các lệnh nêu tên chứ không
            // âm thầm đứng ngoài.
            DirectoryResources::register($registry);

            return $registry;
        });

        $this->app->singleton(TransportManager::class, fn ($app): TransportManager => new TransportManager(
            $app,
            $app->make(Config::class)->get('platform-sync', []),
        ));

        // Kết nối được phân giải MỘT lần và dùng chung cho sổ nhận, con trỏ và
        // báo cáo lệch: ba thứ đó phải nằm cùng transaction với bảng mà
        // projector ghi, nếu không "đã áp" và "đã ghi" tách rời nhau.
        $this->app->singleton(ConnectionInterface::class.'@platform-sync', fn ($app): ConnectionInterface => $app->make(DatabaseManager::class)
            ->connection($app->make(Config::class)->get('platform-sync.connection')));

        $connection = fn ($app): ConnectionInterface => $app->make(ConnectionInterface::class.'@platform-sync');

        $this->app->singleton(InboxStore::class, fn ($app): InboxStore => new InboxStore($connection($app)));
        $this->app->singleton(CursorStore::class, fn ($app): CursorStore => new CursorStore($connection($app)));
        $this->app->singleton(DriftRecorder::class, fn ($app): DriftRecorder => new DriftRecorder($connection($app)));

        $this->app->singleton(EventProcessor::class, fn ($app): EventProcessor => new EventProcessor(
            $app->make(SyncRegistry::class),
            $app->make(InboxStore::class),
            $app->make(DriftRecorder::class),
            $app,
        ));

        $this->app->singleton(FeedPuller::class, fn ($app): FeedPuller => new FeedPuller(
            $app->make(TransportManager::class),
            $app->make(SyncRegistry::class),
            $app->make(CursorStore::class),
            $app->make(EventProcessor::class),
        ));

        $this->app->singleton(Reconciler::class, fn ($app): Reconciler => new Reconciler(
            $app->make(TransportManager::class),
            $app->make(SyncRegistry::class),
            $app->make(DriftRecorder::class),
            $app->make(InboxStore::class),
            $app,
        ));
    }

    public function boot(): void
    {
        // Chế độ áp ở `boot`, sau khi mọi package domain đã kịp khai projector
        // trong `register`. Áp ở `register` thì thứ tự nạp package quyết định
        // loại nào nhận được chế độ của nó — một phụ thuộc vô hình vào thứ tự.
        $this->app->make(SyncRegistry::class)->applyModes(
            $this->app->make(Config::class)->get('platform-sync.modes', []),
        );

        $this->loadMigrationsFrom(__DIR__.'/../database/migrations');

        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__.'/../config/platform-sync.php' => $this->app->configPath('platform-sync.php'),
            ], 'platform-sync-config');

            $this->commands([
                PullCommand::class,
                ReconcileCommand::class,
                StatusCommand::class,
            ]);
        }
    }
}

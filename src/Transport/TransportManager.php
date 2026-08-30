<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

use Godx\Sync\Contracts\Transport;
use Godx\Sync\Exceptions\UnknownTransport;
use Illuminate\Contracts\Container\Container;
use Illuminate\Http\Client\Factory as HttpFactory;

/**
 * Manager pattern của Laravel, có một khác biệt cố ý: KHÔNG kế thừa
 * `Illuminate\Support\Manager`.
 *
 * `Manager` phân giải driver mặc định từ `getDefaultDriver()` rồi cache theo
 * TÊN DRIVER. Ở đây khoá phải là TÊN CẤU HÌNH: hai transport `poll` trỏ về hai
 * Platform khác nhau là chuyện bình thường (staging và production, hoặc hai
 * nhà cung cấp danh tính), và `Manager` sẽ trả nhầm cái thứ nhất cho cả hai.
 *
 * Driver ngoài đăng ký bằng `extend()` — cùng cách `Storage::extend()` làm, nên
 * một consumer thêm RabbitMQ/Kafka không cần sửa package này.
 */
final class TransportManager
{
    /** @var array<string, Transport> */
    private array $resolved = [];

    /** @var array<string, \Closure(array<string, mixed>, Container): Transport> */
    private array $customCreators = [];

    /** @param  array<string, mixed>  $config */
    public function __construct(
        private readonly Container $container,
        private array $config,
    ) {}

    public function transport(?string $name = null): Transport
    {
        $name ??= $this->defaultName();

        return $this->resolved[$name] ??= $this->resolve($name);
    }

    /** @param  \Closure(array<string, mixed>, Container): Transport  $creator */
    public function extend(string $driver, \Closure $creator): void
    {
        $this->customCreators[$driver] = $creator;
    }

    public function defaultName(): string
    {
        return (string) ($this->config['default'] ?? 'poll');
    }

    /** Cho test: thay một transport đã dựng sẵn. */
    public function set(string $name, Transport $transport): void
    {
        $this->resolved[$name] = $transport;
    }

    private function resolve(string $name): Transport
    {
        $config = $this->config['transports'][$name] ?? null;

        if (! is_array($config)) {
            throw UnknownTransport::notConfigured($name);
        }

        $driver = (string) ($config['driver'] ?? $name);

        if (isset($this->customCreators[$driver])) {
            return ($this->customCreators[$driver])($config, $this->container);
        }

        return match ($driver) {
            'array' => new ArrayTransport,
            'poll' => new PollTransport($this->container->make(HttpFactory::class), $config),
            default => throw UnknownTransport::noDriver($driver, $name),
        };
    }
}

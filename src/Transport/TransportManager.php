<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

use Aws\Sqs\SqsClient;
use Godx\Sync\Contracts\Transport;
use Godx\Sync\Exceptions\UnknownTransport;
use Illuminate\Contracts\Config\Repository as Config;
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
 *
 * Cấu hình đọc LƯỜI qua `Config`, không chụp thành mảng lúc dựng. Chụp thì
 * `config()->set('platform-sync.default', ...)` sau khi container đã phân giải
 * manager sẽ không có tác dụng nào — và triệu chứng không phải một lỗi cấu hình
 * mà là một lượt gọi mạng thật tới transport CŨ. Đã mất một vòng gỡ lỗi vì đúng
 * chuyện đó, ở một bài test tưởng là đang dùng driver `array`.
 */
final class TransportManager
{
    /** @var array<string, Transport> */
    private array $resolved = [];

    /** @var array<string, \Closure(array<string, mixed>, Container): Transport> */
    private array $customCreators = [];

    public function __construct(
        private readonly Container $container,
        private readonly Config $config,
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
        return (string) $this->config->get('platform-sync.default', 'sqs');
    }

    /**
     * Mọi transport ĐANG ĐƯỢC KHAI trong cấu hình.
     *
     * Có để một lệnh nói được câu "driver này không làm được việc đó, những cái
     * SAU ĐÂY thì làm được" thay vì câu "driver này không làm được việc đó" rồi
     * bỏ người vận hành lại với một file cấu hình phải tự đọc.
     *
     * @return list<string>
     */
    public function names(): array
    {
        $transports = $this->config->get('platform-sync.transports', []);

        return is_array($transports) ? array_values(array_map(strval(...), array_keys($transports))) : [];
    }

    /** Cho test: thay một transport đã dựng sẵn. */
    public function set(string $name, Transport $transport): void
    {
        $this->resolved[$name] = $transport;
    }

    /**
     * Client SQS dựng từ cấu hình của transport, KHÔNG từ `config/queue.php`.
     *
     * Hai thứ đó trùng tên nhà cung cấp chứ không trùng vai: `queue.sqs` là nơi
     * ứng dụng ĐẨY job của chính nó; ở đây consumer KÉO event của Platform từ
     * một hàng đợi do repo khác sở hữu, thường bằng credential khác. Mượn cấu
     * hình của nhau là cách để một lần đổi queue của app làm câm luồng danh
     * tính, im lặng.
     *
     * Không khai `credentials` là CỐ Ý khi cấu hình bỏ trống: SDK rơi về chuỗi
     * mặc định (biến môi trường → IAM role), và IAM role mới là hình dạng đúng
     * trên hạ tầng thật — ADR 0002 cấm bấm tay trên console, credential dán
     * trong env là cùng một loại nợ.
     *
     * @param  array<string, mixed>  $config
     */
    private static function sqsClient(array $config): SqsClient
    {
        $args = [
            'version' => (string) ($config['version'] ?? 'latest'),
            'region' => (string) ($config['region'] ?? 'ap-northeast-1'),
        ];

        if (($endpoint = $config['endpoint'] ?? null) !== null && $endpoint !== '') {
            $args['endpoint'] = (string) $endpoint;
        }

        $key = $config['key'] ?? null;
        $secret = $config['secret'] ?? null;

        if (is_string($key) && $key !== '' && is_string($secret) && $secret !== '') {
            $args['credentials'] = array_filter([
                'key' => $key,
                'secret' => $secret,
                'token' => $config['token'] ?? null,
            ], static fn (mixed $value): bool => $value !== null && $value !== '');
        }

        return new SqsClient($args);
    }

    private function resolve(string $name): Transport
    {
        $config = $this->config->get("platform-sync.transports.{$name}");

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
            'sqs' => new SqsTransport(self::sqsClient($config), $config),
            default => throw UnknownTransport::noDriver($driver, $name),
        };
    }
}

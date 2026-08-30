<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

use Godx\Sync\Contracts\FetchesResource;
use Godx\Sync\Contracts\PullsChanges;
use Godx\Sync\Contracts\SnapshotsResource;
use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Exceptions\TransportFailure;
use Illuminate\Http\Client\Factory as HttpFactory;
use Illuminate\Http\Client\PendingRequest;
use Illuminate\Http\Client\Response;

/**
 * Consumer CHỦ ĐỘNG kéo. Mặc định của hệ, và không phải vì nó dễ nhất.
 *
 * Webhook đẩy đòi consumer có một địa chỉ công khai mà Platform với tới được.
 * Điều đó loại thẳng mọi consumer sau NAT — workstation trong quán là ví dụ có
 * thật — và bắt Platform giữ một danh sách endpoint cùng cơ chế thử lại cho
 * từng consumer. Kéo thì đảo chiều toàn bộ gánh nặng đó: Platform chỉ cần phục
 * vụ một feed đọc, còn ai kéo được thì kéo.
 *
 * Cái mất là độ trễ (một chu kỳ). Với org/brand/branch/permission, độ trễ vài
 * chục giây không đổi kết quả nghiệp vụ nào — và khi nào nó đổi thì đó là lúc
 * đổi driver, chứ không phải lúc thiết kế lại hệ.
 *
 * ĐIỀU KIỆN HOÁ. Feed dùng ETag: consumer gửi lại `If-None-Match` và Platform
 * trả 304 khi chưa có gì mới. Không có nó, một chu kỳ 60 giây nhân với số
 * consumer nhân với số resource type là một lượng tải cố định mà Platform phải
 * trả mãi mãi để nghe câu "chưa có gì".
 */
final class PollTransport implements FetchesResource, PullsChanges, SnapshotsResource
{
    /** @var array<string, string> */
    private array $etags = [];

    /** @param  array<string, mixed>  $config */
    public function __construct(
        private readonly HttpFactory $http,
        private readonly array $config,
    ) {}

    public function name(): string
    {
        return 'poll';
    }

    public function pull(string $resourceType, ?string $cursor, int $limit): ChangePage
    {
        $etagKey = "changes:{$resourceType}";

        $response = $this->request($etagKey)->get($this->url('changes'), array_filter([
            'type' => $resourceType,
            'cursor' => $cursor,
            'limit' => $limit,
        ], static fn (mixed $value): bool => $value !== null));

        // 304 nghĩa là con trỏ hiện tại vẫn là mới nhất. Trả về trang RỖNG kèm
        // ĐÚNG con trỏ cũ — trả `null` sẽ làm consumer quay về đầu feed và kéo
        // lại toàn bộ lịch sử ở chu kỳ kế.
        if ($response->status() === 304) {
            return ChangePage::empty($cursor);
        }

        $body = $this->decode($response, 'changes');
        $this->rememberEtag($etagKey, $response);

        return new ChangePage(
            events: array_map(
                static fn (array $row): CloudEvent => CloudEvent::fromArray($row),
                array_values($body['events'] ?? []),
            ),
            cursor: isset($body['cursor']) ? (string) $body['cursor'] : $cursor,
            hasMore: (bool) ($body['has_more'] ?? false),
        );
    }

    public function fetch(string $resourceType, string $resourceId): ?CloudEvent
    {
        $response = $this->request()->get($this->url('resource'), [
            'type' => $resourceType,
            'id' => $resourceId,
        ]);

        // 404 là câu trả lời HỢP LỆ: tài nguyên đã bị xoá ở Platform. Ném lỗi ở
        // đây sẽ biến một khe hở giải quyết được thành một hàng đợi tắc.
        if ($response->status() === 404) {
            return null;
        }

        return CloudEvent::fromArray($this->decode($response, 'resource'));
    }

    public function snapshot(string $resourceType, ?string $cursor, int $limit): SnapshotPage
    {
        $response = $this->request()->get($this->url('snapshot'), array_filter([
            'type' => $resourceType,
            'cursor' => $cursor,
            'limit' => $limit,
        ], static fn (mixed $value): bool => $value !== null));

        $body = $this->decode($response, 'snapshot');

        return new SnapshotPage(
            rows: array_map(static fn (array $row): array => [
                'id' => (string) $row['id'],
                'sequence' => (int) ($row['sequence'] ?? 0),
                'tenant_id' => (string) ($row['tenant_id'] ?? ''),
                'data' => $row['data'] ?? [],
            ], array_values($body['rows'] ?? [])),
            cursor: isset($body['cursor']) ? (string) $body['cursor'] : null,
            hasMore: (bool) ($body['has_more'] ?? false),
        );
    }

    private function request(?string $etagKey = null): PendingRequest
    {
        $request = $this->http
            ->timeout((int) ($this->config['timeout'] ?? 10))
            ->connectTimeout((int) ($this->config['connect_timeout'] ?? 5))
            ->retry(
                (int) ($this->config['retries'] ?? 3),
                (int) ($this->config['retry_delay_ms'] ?? 200),
                throw: false,
            )
            ->acceptJson();

        if (($token = $this->config['token'] ?? null) !== null) {
            $request = $request->withToken((string) $token);
        }

        if ($etagKey !== null && isset($this->etags[$etagKey])) {
            $request = $request->withHeaders(['If-None-Match' => $this->etags[$etagKey]]);
        }

        return $request;
    }

    private function rememberEtag(string $key, Response $response): void
    {
        $etag = $response->header('ETag');

        if ($etag !== '') {
            $this->etags[$key] = $etag;
        }
    }

    private function url(string $path): string
    {
        return rtrim((string) ($this->config['endpoint'] ?? ''), '/').'/'.$path;
    }

    /** @return array<string, mixed> */
    private function decode(Response $response, string $what): array
    {
        if ($response->failed()) {
            throw TransportFailure::http($this->name(), $what, $response->status());
        }

        $body = $response->json();

        if (! is_array($body)) {
            throw TransportFailure::body($this->name(), $what);
        }

        return $body;
    }
}

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
use Illuminate\Http\Client\RequestException;
use Illuminate\Http\Client\Response;
use Throwable;

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
 * ĐIỀU KIỆN HOÁ, và CHỈ cho feed `changes`. Feed đó dùng ETag: consumer gửi lại
 * `If-None-Match` và Platform trả 304 khi chưa có gì mới. Không có nó, một chu
 * kỳ 60 giây nhân với số consumer nhân với số resource type là một lượng tải cố
 * định mà Platform phải trả mãi mãi để nghe câu "chưa có gì".
 *
 * `snapshot()` thì CỐ Ý không điều kiện hoá — xem docblock của nó.
 *
 * RETRY chỉ cho thứ thử lại được. `->retry()` trần thử lại MỌI phản hồi không
 * 2xx, mà 404 ở feed `resource` là một câu trả lời HỢP LỆ ("đã xoá ở Platform")
 * — thử lại ba lần là nhân ba tải lên Platform để nghe lại đúng cái nó vừa nói.
 * 401/403/422 cũng vậy theo hướng khác: một credential sai không tự đúng lại
 * sau 200ms. Điều kiện đúng là "không có phản hồi" (lỗi mạng/timeout), 5xx, và
 * 429.
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

    /**
     * Ảnh chụp KHÔNG điều kiện hoá, và 304 ở đây là lỗi phía Platform.
     *
     * Chọn thế chứ không đi thêm ETag cho ảnh chụp, vì hai feed hỏi hai câu
     * khác nhau. `changes` hỏi "có gì mới không" — "không" là một câu trả lời
     * dùng được, và 304 chính là nó. Đối soát hỏi "Platform đang giữ ĐÚNG những
     * gì" — ở đó không có câu trả lời rút gọn nào cả: nó rút kết luận
     * `orphan_local` bằng phép TRỪ TẬP HỢP, nên một trang rỗng đọc thành
     * "Platform không giữ gì", tức mọi hàng cục bộ là mồ côi. Một tối ưu băng
     * thông đổi lấy nguy cơ đó là món hời tồi.
     *
     * Nên: không gửi `If-None-Match`, và từ chối 304 bằng một thông điệp nói
     * đúng chuyện gì xảy ra. Không có nhánh này thì 304 (< 400, nên `failed()`
     * là false) rơi vào `json()` của thân rỗng và nổi lên thành "non-JSON body"
     * — một lời nói dối về nguyên nhân.
     */
    public function snapshot(string $resourceType, ?string $cursor, int $limit): SnapshotPage
    {
        $response = $this->request()->get($this->url('snapshot'), array_filter([
            'type' => $resourceType,
            'cursor' => $cursor,
            'limit' => $limit,
        ], static fn (mixed $value): bool => $value !== null));

        if ($response->status() === 304) {
            throw TransportFailure::unexpectedNotModified($this->name(), 'snapshot');
        }

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
                when: self::retryable(...),
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

    /**
     * Lỗi này có đáng thử lại không.
     *
     * Nhận `null` khi phản hồi không phải `failed()` (304 là ca có thật:
     * `Response::toException()` trả null cho nó). Không có gì hỏng ⇒ không thử
     * lại.
     */
    private static function retryable(?Throwable $exception): bool
    {
        if ($exception === null) {
            return false;
        }

        // Không có phản hồi nào = lỗi mạng/timeout. Đây mới là thứ `retry` sinh
        // ra để chữa, và là ca duy nhất "thử lại y hệt" có cơ hội cho kết quả
        // khác.
        if (! $exception instanceof RequestException) {
            return true;
        }

        $status = $exception->response->status();

        return $status >= 500 || $status === 429;
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

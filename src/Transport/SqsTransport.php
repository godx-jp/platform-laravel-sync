<?php

declare(strict_types=1);

namespace Godx\Sync\Transport;

use Aws\Exception\AwsException;
use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use Godx\Sync\Contracts\AcknowledgesDelivery;
use Godx\Sync\Contracts\PullsChanges;
use Godx\Sync\Envelope\CloudEvent;
use Godx\Sync\Exceptions\MalformedEnvelope;
use Godx\Sync\Exceptions\TransportFailure;
use JsonException;

/**
 * Transport mặc định của hệ: SQS per-consumer sau một SNS topic của Platform.
 *
 * Đây là chân "đường bền" của ADR 0002 (Accepted 2026-08-17): Platform ghi
 * `identity_outbox` trong cùng transaction với thay đổi danh mục, một relay đẩy
 * lên SNS, SNS fanout xuống MỘT hàng đợi cho MỖI consumer, kèm DLQ. Thêm
 * consumer thứ hai là thêm một subscription — không sửa một dòng nào phía
 * Platform.
 *
 * Nó cài `PullsChanges` chứ không đẻ một mặt phẳng mới, vì SQS **là** một hàng
 * đợi consumer KÉO: `ReceiveMessage` với long polling khớp thẳng với vòng
 * `sync:pull` chạy bằng cron + flock, và khớp cả với ràng buộc thật của XServer
 * (không host được tiến trình dài).
 *
 * BỐN CHỖ SQS KHÁC HẲN MỘT FEED HTTP. Cả bốn đều xử lý tường minh ở dưới; đọc
 * docblock của từng phương thức trước khi "sửa" chúng.
 *
 *   1. KHÔNG CÓ CON TRỎ.  `ChangePage::$cursor` mất nghĩa — xem `pull()`.
 *   2. XOÁ LÀ BƯỚC RIÊNG. `pull()` không xoá gì; `ack()` mới xoá — xem
 *      `AcknowledgesDelivery` và `ack()`.
 *   3. VISIBILITY TIMEOUT. Xử lý lâu hơn timeout ⇒ giao hàng lần hai — AN TOÀN,
 *      xem `receive()`.
 *   4. DEAD-LETTER.       Hai loại "hỏng", hai đường ra — xem `quarantine()`.
 *
 * MỘT HÀNG ĐỢI CHỞ MỌI LOẠI TÀI NGUYÊN. SNS fanout đổ tất cả vào cùng một
 * queue, trong khi `FeedPuller::pull()` hỏi theo TỪNG loại. Nên `resourceType`
 * ở đây KHÔNG phải khoá định tuyến: nó là bộ lọc trên thứ vừa nhận được, và
 * message của loại khác được GIỮ LẠI trong bộ nhớ (xem `$stash`) chứ không bị
 * xoá, cũng không bị đẩy về hàng đợi bằng `ChangeMessageVisibility(0)`.
 *
 * Vì sao không đẩy về: mỗi lượt nhận cộng 1 vào `ApproximateReceiveCount`, mà
 * `maxReceiveCount` của redrive policy là thứ quyết định message nào rơi xuống
 * DLQ. Bốn loại tài nguyên nhân với mỗi lần `sync:pull` là bốn lượt nhận cho
 * một message hoàn toàn LÀNH — với `maxReceiveCount: 5` thì hai lượt cron là đủ
 * để đẩy dữ liệu đúng vào DLQ. Giữ trong bộ nhớ thì cả `PullCommand` (nó lặp
 * mọi loại trong CÙNG một tiến trình) chỉ tốn đúng một lượt nhận.
 *
 * Muốn tránh hẳn chuyện đó thì cấu hình ở phía AWS, không phải ở đây: **filter
 * policy trên subscription SNS**, hoặc một queue cho mỗi loại khai ở
 * `transports.sqs.queues`.
 *
 * CHƯA CÓ: XÁC MINH CHỮ KÝ. Nói thẳng, vì một lớp trông như đã canh còn tệ hơn
 * không có lớp nào. Package này **không** verify gì trên envelope — không JWKS,
 * không RFC 9421, không `Aws\Sns\MessageValidator`. Điều duy nhất đứng giữa
 * "envelope này do Platform phát" và "consumer tin nó" là **IAM**: chỉ SNS topic
 * mới có quyền ghi vào queue, và chỉ consumer mới có quyền đọc.
 *
 * Đó là một lập luận thật (kênh SNS→SQS nằm trong mạng AWS, không đi qua
 * Internet công khai), nhưng nó KHÔNG phải thứ ADR 0002 đòi ở mục "khoá ký +
 * đường xoay khoá (JWKS)", và nó không phủ chân còn lại của ADR — **SNS → HTTPS
 * trực tiếp** cho CAEP, nơi payload ĐI QUA Internet và `MessageValidator` là bắt
 * buộc. Chân đó chưa được cài ở đâu trong package này.
 *
 * Chỗ tự nhiên để cắm về sau, ghi lại để người sau không phải đi tìm: ngay
 * trong `decode()`, ở đúng nhánh mở bao SNS — đó là chỗ duy nhất phong bì SNS
 * (`Signature`, `SigningCertURL`, `Type`) còn nguyên vẹn trước khi bị bóc.
 * Message có `RawMessageDelivery` bật thì KHÔNG mang phong bì đó, nên một lớp
 * verify cắm vào đây phải quyết định trước: bắt buộc phong bì (và cấm
 * `RawMessageDelivery`), hay chấp nhận cả hai (và thế thì nó không còn là một
 * cổng nữa). Đừng cài nửa vời.
 */
final class SqsTransport implements AcknowledgesDelivery, PullsChanges
{
    /** Trần cứng của `ReceiveMessage`, không phải lựa chọn của package. */
    private const MAX_BATCH = 10;

    /**
     * event id → biên nhận đang cầm.
     *
     * Khoá là CloudEvents `id` vì đó là thứ duy nhất người gọi cầm được:
     * `FeedPuller` chỉ nhìn thấy envelope, không nhìn thấy message của SQS. Đây
     * cũng chính là khoá chống trùng của sổ nhận, nên hai bên nói cùng một
     * danh tính.
     *
     * @var array<string, array{queue: string, receipt: string}>
     */
    private array $receipts = [];

    /**
     * Envelope đã nhận được nhưng thuộc loại tài nguyên khác với loại đang hỏi.
     *
     * @var array<string, list<CloudEvent>>
     */
    private array $stash = [];

    /**
     * Message không dựng nổi thành envelope, cùng lý do — để `sync:status` và
     * người vận hành đọc được thay vì phải đi mò log.
     *
     * @var list<array{message_id: string, reason: string, quarantined: bool}>
     */
    private array $poison = [];

    /** @param  array<string, mixed>  $config */
    public function __construct(
        private readonly SqsClient $sqs,
        private readonly array $config,
    ) {}

    public function name(): string
    {
        return 'sqs';
    }

    /**
     * CON TRỎ KHÔNG TỒN TẠI Ở ĐÂY, và trả `null` là câu trả lời ĐÚNG.
     *
     * Vị trí của một consumer SQS không phải một giá trị nó lưu — nó là trạng
     * thái của chính hàng đợi: message đã xoá thì không bao giờ quay lại,
     * message chưa xoá thì sẽ quay lại. Không có gì để ghi nhớ, nên bịa ra một
     * chuỗi (message id cuối, timestamp) rồi cất vào `platform_sync_cursors`
     * sẽ tạo ra một con số trông như vị trí mà không tầng nào đọc — đúng loại
     * dữ liệu người sau sẽ tin.
     *
     * `$cursor` nhận vào bị BỎ QUA có chủ đích, không phải bị quên. Không có
     * cách nào tua một hàng đợi tới một vị trí; muốn phát lại thì đó là việc
     * của SNS→Firehose→S3 (ADR 0002 §2), không phải của driver này.
     *
     * ⚠️ `null` ở đây KHÔNG được đọc thành "quay về đầu feed". Với
     * `PollTransport` thì null đúng là như thế — chính vì vậy nó cẩn thận trả
     * lại con trỏ CŨ khi gặp 304. Hai nghĩa trái ngược nhau sống chung được vì
     * `CursorStore` khoá theo (transport, loại tài nguyên): một `null` do `sqs`
     * ghi không bao giờ được `poll` đọc lên. Đừng gộp khoá đó lại.
     *
     * `hasMore` thì phải SUY RA, khác hẳn feed HTTP. Docblock của `ChangePage`
     * cảnh báo đúng cho một feed có khả năng tự khai `has_more`; SQS thì không
     * có gì để khai — `ApproximateNumberOfMessages` là một lượt gọi API khác và
     * cái tên của nó đã nói nó xấp xỉ. Suy ra "trang đầy = còn nữa" ở đây tốn
     * thêm nhiều nhất MỘT lượt long-poll rỗng, mà một lượt long-poll rỗng chính
     * là hình dạng bình thường của việc chờ trên hàng đợi — không phải một vòng
     * quay nóng.
     */
    public function pull(string $resourceType, ?string $cursor, int $limit): ChangePage
    {
        $batch = max(1, min(self::MAX_BATCH, $limit));
        $queue = $this->queueFor($resourceType);

        // Hàng còn giữ từ lượt hỏi loại khác được phục vụ TRƯỚC — chúng đã tốn
        // một lượt nhận rồi, và biên nhận của chúng đang đếm ngược.
        $events = $this->takeStashed($resourceType, $batch);
        $queueWasFull = false;

        if (count($events) < $batch) {
            $messages = $this->receive($queue, $batch);
            $queueWasFull = count($messages) === $batch;

            foreach ($messages as $message) {
                $event = $this->decode($queue, $message);

                if ($event === null) {
                    continue;
                }

                $this->receipts[$event->id] = ['queue' => $queue, 'receipt' => (string) $message['ReceiptHandle']];

                if ($event->resourceType() === $resourceType) {
                    $events[] = $event;
                } else {
                    $this->stash[$event->resourceType()][] = $event;
                }
            }
        }

        return new ChangePage(
            events: array_values($events),
            cursor: null,
            hasMore: $queueWasFull || ($this->stash[$resourceType] ?? []) !== [],
        );
    }

    /**
     * XOÁ, và chỉ khi envelope đã có kết cục trong sổ nhận.
     *
     * Đường an toàn hai chiều, cả hai đều đã tính:
     *
     *  - Chết SAU khi chiếu, TRƯỚC khi xoá ⇒ message hiện lại, giao lần hai,
     *    `InboxStore::claim()` thua cuộc đua khoá chính ⇒ `Duplicate` ⇒ settled
     *    ⇒ xoá. Không mất gì, không áp hai lần.
     *  - Chết TRƯỚC khi chiếu ⇒ hàng `claimed` còn treo trong sổ nhận và
     *    `sync:status` gọi nó là STUCK; message cũng hiện lại. Đây là công việc
     *    dở dang duy nhất hệ không tự nhận ra, nên nó phải nhìn thấy được.
     *
     * Nuốt biên nhận đã hết hiệu lực là CỐ Ý. Khi visibility timeout hết hạn
     * giữa lúc đang xử lý, message được giao lần hai và bản thứ hai (verdict
     * `Duplicate`) xoá nó bằng biên nhận MỚI của nó; bản thứ nhất xong sau và
     * cầm một biên nhận đã chết. Ném ở đó là làm đỏ một lượt kéo hoàn toàn
     * đúng, vì một message đã ở đúng trạng thái ta muốn: đã biến mất.
     */
    public function ack(string $eventId): void
    {
        $held = $this->receipts[$eventId] ?? null;

        // Envelope không đến từ hàng đợi này. Đối soát tự dựng envelope
        // (`urn:godx:sync:reconcile`) và nó vẫn đi qua sổ nhận như thường.
        if ($held === null) {
            return;
        }

        unset($this->receipts[$eventId]);

        try {
            $this->sqs->deleteMessage(['QueueUrl' => $held['queue'], 'ReceiptHandle' => $held['receipt']]);
        } catch (SqsException $e) {
            if (! self::receiptAlreadyGone($e)) {
                throw TransportFailure::queue($this->name(), 'deleting a settled message', $e->getAwsErrorCode() ?? $e->getMessage());
            }
        } catch (AwsException $e) {
            throw TransportFailure::queue($this->name(), 'deleting a settled message', $e->getAwsErrorCode() ?? $e->getMessage());
        }
    }

    /**
     * KHÔNG xoá, KHÔNG rút visibility về 0 — chỉ buông biên nhận.
     *
     * Message hiện lại khi visibility timeout hết hạn, `ApproximateReceiveCount`
     * cộng một, và sau `maxReceiveCount` lượt SQS tự chuyển nó sang DLQ. Đó
     * chính là cơ chế dead-letter mà ADR 0002 mua bằng redrive policy; dựng lại
     * nó trong PHP là dựng lại một thứ đã có, ở nơi không có độ bền.
     */
    public function abandon(string $eventId, string $reason): void
    {
        unset($this->receipts[$eventId]);
    }

    /**
     * Message không dựng nổi thành envelope, cùng lý do.
     *
     * @return list<array{message_id: string, reason: string, quarantined: bool}>
     */
    public function poisonSeen(): array
    {
        return $this->poison;
    }

    /**
     * VISIBILITY TIMEOUT: đặt tường minh, và giao hàng hai lần là AN TOÀN.
     *
     * Nếu một lượt `sync:pull` xử lý lâu hơn timeout, SQS đưa message trở lại
     * và một tiến trình khác nhận nó trong khi tiến trình đầu vẫn đang chạy.
     * Điều đó KHÔNG hỏng, và đừng "sửa" bằng cách dựng heartbeat kéo dài
     * visibility: `InboxStore::claim()` là một INSERT có khoá chính trên
     * `event_id`, chạy TRƯỚC khi projector chạy, nên đúng một tiến trình thắng
     * và tiến trình kia nhận `Duplicate`. Giao hàng at-least-once là hợp đồng
     * đã ký của SQS, và hàm chiếu đã idempotent theo yêu cầu của `Projector`.
     *
     * Cái phải canh là chiều ngược lại: timeout NGẮN hơn một lượt xử lý bình
     * thường thì mọi message đều được giao hai lần và một nửa lưu lượng trở
     * thành `Duplicate` — vẫn đúng, nhưng lãng phí và làm mọi con số khó đọc.
     * Mặc định 60s cho một lượt cron `--max-time=55`.
     *
     * Đặt ở đây, trên từng lượt nhận, chứ không dựa vào thuộc tính của queue:
     * queue do Terraform bên `../id` sở hữu, còn thời gian xử lý là chuyện của
     * consumer — bên biết câu trả lời phải là bên nói ra nó.
     *
     * @return list<array<string, mixed>>
     */
    private function receive(string $queue, int $batch): array
    {
        try {
            $result = $this->sqs->receiveMessage([
                'QueueUrl' => $queue,
                'MaxNumberOfMessages' => $batch,
                // Long polling. `0` (short polling) trả rỗng kể cả khi hàng đợi
                // có message, vì nó chỉ hỏi một tập con máy chủ — và một vòng
                // cron thấy rỗng sẽ kết luận "không có gì mới".
                'WaitTimeSeconds' => max(0, min(20, (int) ($this->config['wait_time_seconds'] ?? 20))),
                'VisibilityTimeout' => max(1, (int) ($this->config['visibility_timeout'] ?? 60)),
                'MessageSystemAttributeNames' => ['ApproximateReceiveCount'],
                'MessageAttributeNames' => ['All'],
            ]);
        } catch (AwsException $e) {
            throw TransportFailure::queue($this->name(), 'receiving from the queue', $e->getAwsErrorCode() ?? $e->getMessage());
        }

        $messages = $result['Messages'] ?? [];

        return is_array($messages) ? array_values($messages) : [];
    }

    /**
     * Thân message → envelope, hoặc null nếu nó vào diện cách ly.
     *
     * MỞ BAO SNS. Khi subscription KHÔNG bật `RawMessageDelivery`, SNS gói
     * payload vào một phong bì của riêng nó (`{"Type":"Notification",
     * "Message":"<chuỗi JSON>"}`) — tức `specversion` nằm sâu thêm một lớp và
     * `CloudEvent::fromArray()` sẽ từ chối MỌI message với lý do "thiếu
     * specversion". Đó là một sai cấu hình một-dòng ở phía AWS mà triệu chứng
     * đọc lên như lỗi của Platform, nên driver mở cả hai kiểu bao và không bắt
     * ai phải đoán.
     *
     * MỘT MESSAGE HỎNG KHÔNG ĐƯỢC DỪNG CẢ LÔ. Trả null rồi đi tiếp, thay vì
     * ném: một envelope rác nằm giữa chín envelope tốt mà làm hỏng cả lượt kéo
     * thì nó chặn đúng thứ nó không liên quan.
     *
     * @param  array<string, mixed>  $message
     */
    private function decode(string $queue, array $message): ?CloudEvent
    {
        try {
            $body = json_decode((string) ($message['Body'] ?? ''), true, 512, JSON_THROW_ON_ERROR);

            if (is_array($body) && ($body['Type'] ?? null) === 'Notification' && isset($body['Message'])) {
                $body = json_decode((string) $body['Message'], true, 512, JSON_THROW_ON_ERROR);
            }

            if (! is_array($body)) {
                throw new JsonException('Message body is not a JSON object.');
            }

            return CloudEvent::fromArray($body);
        } catch (JsonException|MalformedEnvelope $e) {
            $this->quarantine($queue, $message, $e->getMessage());

            return null;
        }
    }

    /**
     * DEAD-LETTER, chân thứ nhất: envelope KHÔNG dựng nổi.
     *
     * Có hai loại "hỏng lặp lại" và chúng đi hai đường khác nhau — gộp lại là
     * sai một trong hai:
     *
     *  - Projector đổ (`Verdict::Failed`) — envelope hợp lệ, sổ nhận có hàng,
     *    lý do đã ghi. Driver KHÔNG xoá; redrive policy của SQS đưa nó sang DLQ
     *    sau `maxReceiveCount` lượt. Đường này không cần mã nào ở đây.
     *  - Thân message không thành envelope — KHÔNG có event id, nên sổ nhận
     *    không bao giờ có hàng cho nó và nó không bao giờ "settled". Nếu chỉ để
     *    đó, nó quay lại đủ `maxReceiveCount` lượt rồi sang DLQ mà KHÔNG mang
     *    theo một chữ nào về lý do — người vận hành mở DLQ ra và thấy một thân
     *    message y hệt cái đang nằm trong queue. Nên driver tự cách ly nó NGAY,
     *    kèm lý do trong message attribute.
     *
     * KHÔNG CÓ DLQ THÌ KHÔNG XOÁ. Xoá một thứ không sao chép được đi đâu là mất
     * dữ liệu do chính ta gây ra, để đổi lấy một hàng đợi sạch mắt. Để nguyên
     * thì redrive policy vẫn xử lý được, và trên queue chuẩn (không FIFO) một
     * message đang ẩn KHÔNG chặn message phía sau — thứ tự không được giả định
     * ở đây ngay từ đầu (ADR 0002, *Bất biến*).
     *
     * @param  array<string, mixed>  $message
     */
    private function quarantine(string $queue, array $message, string $reason): void
    {
        $dlq = $this->config['dead_letter_queue_url'] ?? null;
        $messageId = (string) ($message['MessageId'] ?? 'unknown');

        if (! is_string($dlq) || $dlq === '') {
            $this->poison[] = ['message_id' => $messageId, 'reason' => $reason, 'quarantined' => false];

            return;
        }

        $this->sqs->sendMessage([
            'QueueUrl' => $dlq,
            'MessageBody' => (string) ($message['Body'] ?? ''),
            'MessageAttributes' => [
                'QuarantineReason' => ['DataType' => 'String', 'StringValue' => mb_substr($reason, 0, 250)],
                'SourceQueueUrl' => ['DataType' => 'String', 'StringValue' => $queue],
                'QuarantinedBy' => ['DataType' => 'String', 'StringValue' => 'godx-jp/platform-laravel-sync'],
            ],
        ]);

        $this->sqs->deleteMessage(['QueueUrl' => $queue, 'ReceiptHandle' => (string) $message['ReceiptHandle']]);

        $this->poison[] = ['message_id' => $messageId, 'reason' => $reason, 'quarantined' => true];
    }

    /**
     * @return list<CloudEvent>
     */
    private function takeStashed(string $resourceType, int $limit): array
    {
        $held = $this->stash[$resourceType] ?? [];

        if ($held === []) {
            return [];
        }

        $taken = array_slice($held, 0, $limit);
        $this->stash[$resourceType] = array_values(array_slice($held, count($taken)));

        return array_values($taken);
    }

    /**
     * Queue của một loại tài nguyên: bản khai riêng nếu có, nếu không là hàng
     * đợi chung của consumer.
     *
     * Thiếu cả hai thì hỏng TO TIẾNG ngay tại đây, chứ không dựng client rồi
     * gọi AWS với `QueueUrl` rỗng — lỗi trả về khi ấy là một mã của AWS nói về
     * tham số, không nói gì về cấu hình còn thiếu.
     */
    private function queueFor(string $resourceType): string
    {
        $perType = $this->config['queues'] ?? [];
        $url = is_array($perType) ? ($perType[$resourceType] ?? null) : null;
        $url ??= $this->config['queue_url'] ?? null;

        if (! is_string($url) || $url === '') {
            throw TransportFailure::misconfigured(
                $this->name(),
                "no queue URL for resource type [{$resourceType}]. Set platform-sync.transports.sqs.queue_url (PLATFORM_SYNC_SQS_QUEUE_URL) for the consumer's shared queue, or platform-sync.transports.sqs.queues['{$resourceType}'] for a dedicated one.",
            );
        }

        return $url;
    }

    /**
     * Biên nhận đã hết hiệu lực hoặc message đã biến mất.
     *
     * `ReceiptHandleIsInvalid` là ca biên nhận không còn dùng được;
     * `InvalidParameterValue` là ca AWS trả về khi biên nhận đã quá hạn theo
     * cách khác. Cả hai đều nghĩa là message không còn ở trạng thái ta cần xoá
     * — tức việc đã xong.
     */
    private static function receiptAlreadyGone(SqsException $e): bool
    {
        return in_array($e->getAwsErrorCode(), ['ReceiptHandleIsInvalid', 'InvalidParameterValue'], true);
    }
}

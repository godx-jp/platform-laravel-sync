<?php

declare(strict_types=1);

namespace Godx\Sync\Envelope;

use DateTimeImmutable;
use Godx\Sync\Exceptions\MalformedEnvelope;

/**
 * CloudEvents 1.0 envelope (https://github.com/cloudevents/spec).
 *
 * Vì sao là CloudEvents chứ không phải một envelope tự chế: nó có spec công
 * khai, có binding cho HTTP/AMQP/Kafka/NATS, và có thư viện ở mọi ngôn ngữ.
 * Một envelope tự chế thì mỗi consumer mới lại phải đọc mã nguồn của Platform
 * để biết trường nào bắt buộc — và câu trả lời sẽ trôi.
 *
 * HAI TRƯỜNG MỞ RỘNG mà hệ này bắt buộc, cả hai đều là extension hợp lệ của
 * spec (chữ thường, không dấu, kiểu string):
 *
 *   sequence  — ĐƠN ĐIỆU theo `subject`. Đây là thứ duy nhất cho phép consumer
 *               phân biệt "event đến muộn" với "event mới". Thiếu nó thì mọi
 *               phép chống trùng chỉ chặn được bản sao y hệt, còn hai bản cập
 *               nhật của CÙNG một tài nguyên đến sai thứ tự sẽ ghi đè nhau —
 *               im lặng, và bản thắng là bản đến sau chứ không phải bản mới.
 *   tenantid  — tổ chức sở hữu tài nguyên. Consumer đa tổ chức PHẢI lọc theo
 *               nó trước khi ghi; thiếu nó thì một event của org khác vẫn
 *               trông hợp lệ.
 *
 * `time` được giữ nguyên là thời điểm Platform PHÁT, không phải lúc consumer
 * nhận — hai cái đó lệch nhau khi transport chậm hoặc replay, và trộn chúng
 * làm hỏng mọi phép đo độ trễ.
 */
final readonly class CloudEvent
{
    public const SPEC_VERSION = '1.0';

    /**
     * @param  array<string, mixed>  $data
     * @param  array<string, string>  $extensions
     */
    public function __construct(
        public string $id,
        public string $source,
        public string $type,
        public string $subject,
        public DateTimeImmutable $time,
        public array $data,
        public int $sequence,
        public string $tenantId,
        public ?string $dataSchema = null,
        public array $extensions = [],
    ) {}

    /**
     * @param  array<string, mixed>  $payload
     */
    public static function fromArray(array $payload): self
    {
        $specVersion = $payload['specversion'] ?? null;

        if ($specVersion !== self::SPEC_VERSION) {
            throw MalformedEnvelope::specVersion(is_string($specVersion) ? $specVersion : gettype($specVersion));
        }

        foreach (['id', 'source', 'type', 'subject', 'time', 'sequence', 'tenantid'] as $required) {
            if (! isset($payload[$required]) || $payload[$required] === '') {
                throw MalformedEnvelope::missing($required);
            }
        }

        if (! is_array($payload['data'] ?? null)) {
            throw MalformedEnvelope::missing('data');
        }

        $sequence = $payload['sequence'];

        // Spec nói extension là string; nhưng một Platform gửi số nguyên JSON
        // vẫn là ý định rõ ràng, nên chấp nhận cả hai rồi ép về int. Cái KHÔNG
        // chấp nhận là chuỗi không phải số — đó là lỗi lập trình phía phát, và
        // nuốt nó sẽ biến mọi sequence thành 0, tức tắt luôn phép kiểm thứ tự.
        if (! is_int($sequence) && ! (is_string($sequence) && preg_match('/^\d+$/', $sequence) === 1)) {
            throw MalformedEnvelope::sequence(is_scalar($sequence) ? (string) $sequence : gettype($sequence));
        }

        try {
            $time = new DateTimeImmutable((string) $payload['time']);
        } catch (\Exception) {
            throw MalformedEnvelope::time((string) $payload['time']);
        }

        $known = ['specversion', 'id', 'source', 'type', 'subject', 'time', 'data', 'sequence', 'tenantid', 'dataschema', 'datacontenttype'];

        return new self(
            id: (string) $payload['id'],
            source: (string) $payload['source'],
            type: (string) $payload['type'],
            subject: (string) $payload['subject'],
            time: $time,
            data: $payload['data'],
            sequence: (int) $sequence,
            tenantId: (string) $payload['tenantid'],
            dataSchema: isset($payload['dataschema']) ? (string) $payload['dataschema'] : null,
            extensions: array_map(
                static fn (mixed $value): string => is_scalar($value) ? (string) $value : json_encode($value, JSON_THROW_ON_ERROR),
                array_diff_key($payload, array_flip($known)),
            ),
        );
    }

    /**
     * Danh tính tài nguyên, tách khỏi `subject`.
     *
     * `subject` theo quy ước là `<resource>/<id>`. Trả về phần id; nếu không có
     * dấu gạch chéo thì chính subject LÀ id — đừng ném lỗi ở đây, vì một
     * Platform khác có quyền dùng subject phẳng và envelope vẫn hợp lệ.
     */
    public function resourceId(): string
    {
        $slash = strrpos($this->subject, '/');

        return $slash === false ? $this->subject : substr($this->subject, $slash + 1);
    }

    /**
     * Loại tài nguyên suy ra từ `type`.
     *
     * `godx.directory.branch.updated` → `godx.directory.branch`. Registry khai
     * theo LOẠI chứ không theo từng động từ, nên `created`/`updated`/`deleted`
     * cùng đi vào một projector — projector nhận cả `verb()` để rẽ nhánh.
     */
    public function resourceType(): string
    {
        $dot = strrpos($this->type, '.');

        return $dot === false ? $this->type : substr($this->type, 0, $dot);
    }

    /**
     * `prevsequence` — sequence liền trước của CÙNG subject, nếu Platform gửi.
     *
     * Đây là thứ biến phát hiện khe hở từ phỏng đoán thành phép đo. Nếu chỉ có
     * `sequence` đơn điệu, consumer không thể phân biệt "Platform bỏ số 48210"
     * với "event 48210 bị mất trên đường" — nên nó buộc phải hoặc bỏ qua khe
     * hở (mất dữ liệu) hoặc đòi số liên tiếp (ràng buộc Platform không giữ nổi
     * khi có nhiều tiến trình phát).
     *
     * Có chuỗi này thì mỗi event tự nói nó nối vào đâu, và Platform vẫn được tự
     * do đánh số thưa.
     *
     * Vắng mặt là HỢP LỆ, và hệ quả phải nói rõ: khi vắng, phát hiện khe hở tắt
     * cho subject đó và chân duy nhất còn lại là đối soát định kỳ.
     */
    public function previousSequence(): ?int
    {
        $value = $this->extensions['prevsequence'] ?? null;

        if ($value === null || preg_match('/^\\d+$/', $value) !== 1) {
            return null;
        }

        return (int) $value;
    }

    public function verb(): string
    {
        $dot = strrpos($this->type, '.');

        return $dot === false ? '' : substr($this->type, $dot + 1);
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return array_filter([
            'specversion' => self::SPEC_VERSION,
            'id' => $this->id,
            'source' => $this->source,
            'type' => $this->type,
            'subject' => $this->subject,
            'time' => $this->time->format(DATE_RFC3339_EXTENDED),
            'datacontenttype' => 'application/json',
            'dataschema' => $this->dataSchema,
            'sequence' => (string) $this->sequence,
            'tenantid' => $this->tenantId,
            'data' => $this->data,
        ], static fn (mixed $value): bool => $value !== null) + $this->extensions;
    }
}

<?php

declare(strict_types=1);

namespace Godx\Sync\Tests\Fixtures;

use Aws\CommandInterface;
use Aws\Result;
use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;
use GuzzleHttp\Promise\Create;
use GuzzleHttp\Promise\PromiseInterface;
use Psr\Http\Message\RequestInterface;

/**
 * Một hàng đợi SQS giả, cắm vào ĐÚNG khe `handler` của SDK.
 *
 * Cắm ở đó chứ không bọc `SqsClient` bằng một interface của riêng ta, vì mọi
 * tầng middleware thật của SDK vẫn chạy — trong đó có tầng validate tham số
 * theo mô hình API. Đo được (tiêm lỗi, 2026-08-31): nó bắt **tham số bắt buộc
 * còn thiếu** (bỏ `QueueUrl`) và **sai kiểu** (`WaitTimeSeconds => 'twenty'`).
 *
 * ⚠️ Nó KHÔNG bắt tham số THỪA: `'SystemAttributeNames'` gõ sai tên đi lọt qua
 * validate và bị bỏ im lặng. Đừng tin tầng đó cho chiều ấy — bài
 * *"asks for long polling and an explicit visibility timeout"* mới là chỗ ghim
 * từng tên tham số, bằng cách đọc thẳng lệnh đã ghi lại.
 *
 * Không có gói tin nào rời khỏi tiến trình: handler trả `Aws\Result` thẳng.
 */
final class FakeSqs
{
    /** @var list<array{name: string, args: array<string, mixed>}> */
    public array $calls = [];

    /** @var list<list<array<string, mixed>>> Lô message cho từng lượt ReceiveMessage. */
    public array $receives = [];

    /** Mã lỗi AWS mà DeleteMessage sẽ ném, hoặc null. */
    public ?string $deleteErrorCode = null;

    public SqsClient $client;

    public function __construct()
    {
        $this->client = new SqsClient([
            'region' => 'ap-northeast-1',
            'version' => 'latest',
            'credentials' => ['key' => 'fake', 'secret' => 'fake'],
            'handler' => $this->handler(...),
        ]);
    }

    /** @param  list<array<string, mixed>>  $messages */
    public function willReceive(array $messages): self
    {
        $this->receives[] = $messages;

        return $this;
    }

    /** @return list<array<string, mixed>> */
    public function callsOf(string $name): array
    {
        return array_values(array_map(
            static fn (array $call): array => $call['args'],
            array_filter($this->calls, static fn (array $call): bool => $call['name'] === $name),
        ));
    }

    public function countOf(string $name): int
    {
        return count($this->callsOf($name));
    }

    private function handler(CommandInterface $command, RequestInterface $request): PromiseInterface
    {
        $this->calls[] = ['name' => $command->getName(), 'args' => $command->toArray()];

        return match ($command->getName()) {
            'ReceiveMessage' => Create::promiseFor(new Result(['Messages' => array_shift($this->receives) ?? []])),
            'DeleteMessage' => $this->deleteErrorCode === null
                ? Create::promiseFor(new Result([]))
                : Create::rejectionFor(new SqsException(
                    'The receipt handle has expired.',
                    $command,
                    ['code' => $this->deleteErrorCode],
                )),
            'SendMessage' => Create::promiseFor(new Result(['MessageId' => 'dlq-message'])),
            default => Create::promiseFor(new Result([])),
        };
    }
}

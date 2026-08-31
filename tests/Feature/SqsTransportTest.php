<?php

declare(strict_types=1);

use Godx\Sync\Exceptions\TransportFailure;
use Godx\Sync\Tests\Fixtures\FakeSqs;
use Godx\Sync\Transport\SqsTransport;

const MAIN_QUEUE = 'https://sqs.ap-northeast-1.amazonaws.com/1/tempo-identity-events';
const DLQ = 'https://sqs.ap-northeast-1.amazonaws.com/1/tempo-identity-events-dlq';

/** @param  array<string, mixed>  $config */
function sqsTransport(FakeSqs $fake, array $config = []): SqsTransport
{
    return new SqsTransport($fake->client, array_merge(['queue_url' => MAIN_QUEUE], $config));
}

/** @return array<string, mixed> */
function sqsEnvelope(string $id = 'br_1', int $sequence = 5, string $type = 'godx.directory.branch'): array
{
    return [
        'specversion' => '1.0',
        'id' => 'evt_'.$id.'_'.$sequence,
        'source' => 'https://id.godx.jp',
        'type' => $type.'.updated',
        'subject' => $type.'/'.$id,
        'time' => '2026-08-31T04:12:07Z',
        'sequence' => (string) $sequence,
        'tenantid' => 'org_1',
        'data' => ['id' => $id, 'name' => 'Hongo'],
    ];
}

/** @param  array<string, mixed>|string  $body */
function sqsMessage(array|string $body, string $receipt = 'rh-1', string $messageId = 'msg-1'): array
{
    return [
        'MessageId' => $messageId,
        'ReceiptHandle' => $receipt,
        'Body' => is_string($body) ? $body : json_encode($body, JSON_THROW_ON_ERROR),
    ];
}

// ─── Đọc envelope ra khỏi hàng đợi ─────────────────────────────────────────

it('turns a queue message into an envelope', function (): void {
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    $page = sqsTransport($fake)->pull('godx.directory.branch', null, 10);

    expect($page->events)->toHaveCount(1)
        ->and($page->events[0]->resourceId())->toBe('br_1')
        ->and($page->events[0]->sequence)->toBe(5);
});

it('unwraps the SNS notification envelope when raw message delivery is off', function (): void {
    // Không bật `RawMessageDelivery` thì SNS gói payload thêm một lớp, và
    // `CloudEvent::fromArray()` sẽ từ chối MỌI message với lý do "thiếu
    // specversion" — một sai cấu hình một dòng ở AWS đọc lên như lỗi của
    // Platform.
    $fake = (new FakeSqs)->willReceive([sqsMessage([
        'Type' => 'Notification',
        'MessageId' => 'sns-1',
        'TopicArn' => 'arn:aws:sns:ap-northeast-1:1:godx-identity-events',
        'Message' => json_encode(sqsEnvelope(), JSON_THROW_ON_ERROR),
    ])]);

    $page = sqsTransport($fake)->pull('godx.directory.branch', null, 10);

    expect($page->events)->toHaveCount(1)
        ->and($page->events[0]->resourceId())->toBe('br_1');
});

it('asks for long polling and an explicit visibility timeout', function (): void {
    $fake = new FakeSqs;

    sqsTransport($fake, ['wait_time_seconds' => 20, 'visibility_timeout' => 90])
        ->pull('godx.directory.branch', null, 10);

    $args = $fake->callsOf('ReceiveMessage')[0];

    expect($args['WaitTimeSeconds'])->toBe(20)
        ->and($args['VisibilityTimeout'])->toBe(90)
        ->and($args['QueueUrl'])->toBe(MAIN_QUEUE)
        ->and($args['MaxNumberOfMessages'])->toBe(10)
        // Tầng validate của SDK bỏ IM LẶNG một tham số gõ sai tên, nên chỗ ghim
        // tên tham số là ở đây chứ không ở đó.
        ->and($args['MessageSystemAttributeNames'])->toBe(['ApproximateReceiveCount']);
});

// ─── Quyết định 1: con trỏ ─────────────────────────────────────────────────

it('returns a null cursor, because a queue has no position to remember', function (): void {
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    expect(sqsTransport($fake)->pull('godx.directory.branch', null, 10)->cursor)->toBeNull();
});

it('ignores the cursor it is handed instead of pretending to seek', function (): void {
    // Không có cách nào tua một hàng đợi tới một vị trí. Bịa ra một giá trị rồi
    // cất vào `platform_sync_cursors` sẽ tạo một con số trông như vị trí mà
    // không tầng nào đọc.
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    $page = sqsTransport($fake)->pull('godx.directory.branch', 'c-999', 10);

    expect($page->events)->toHaveCount(1)
        ->and($page->cursor)->toBeNull()
        ->and($fake->callsOf('ReceiveMessage')[0])->not->toHaveKey('ReceiveRequestAttemptId');
});

// ─── Quyết định 2: xoá là bước RIÊNG ───────────────────────────────────────

it('deletes nothing while pulling', function (): void {
    // Xoá trong `pull()` nghĩa là một lần chết giữa lúc chiếu làm event bốc hơi
    // vĩnh viễn — không còn bản nào ở đâu để giao lại.
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    sqsTransport($fake)->pull('godx.directory.branch', null, 10);

    expect($fake->countOf('DeleteMessage'))->toBe(0);
});

it('deletes exactly the acknowledged message, by its own receipt handle', function (): void {
    $fake = (new FakeSqs)->willReceive([
        sqsMessage(sqsEnvelope('br_1', 5), 'rh-one', 'msg-1'),
        sqsMessage(sqsEnvelope('br_2', 6), 'rh-two', 'msg-2'),
    ]);

    $transport = sqsTransport($fake);
    $page = $transport->pull('godx.directory.branch', null, 10);
    $transport->ack($page->events[1]->id);

    expect($fake->countOf('DeleteMessage'))->toBe(1)
        ->and($fake->callsOf('DeleteMessage')[0]['ReceiptHandle'])->toBe('rh-two')
        ->and($fake->callsOf('DeleteMessage')[0]['QueueUrl'])->toBe(MAIN_QUEUE);
});

it('ignores an ack for an event that never came from this queue', function (): void {
    // Đối soát tự dựng envelope (`urn:godx:sync:reconcile`) và nó vẫn đi qua sổ
    // nhận như thường.
    $fake = new FakeSqs;

    sqsTransport($fake)->ack('evt_synthesised_by_reconcile');

    expect($fake->countOf('DeleteMessage'))->toBe(0);
});

it('does not delete an abandoned message, so the queue can redeliver it', function (): void {
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    $transport = sqsTransport($fake);
    $page = $transport->pull('godx.directory.branch', null, 10);
    $transport->abandon($page->events[0]->id, 'projector exploded');

    expect($fake->countOf('DeleteMessage'))->toBe(0);
});

// ─── Quyết định 3: visibility timeout ──────────────────────────────────────

it('swallows an expired receipt handle, because the message is already gone', function (): void {
    // Xử lý lâu hơn visibility timeout ⇒ message được giao lần hai; bản thứ hai
    // (verdict `duplicate`) xoá nó bằng biên nhận MỚI, còn bản đầu xong sau và
    // cầm một biên nhận đã chết. Ném ở đó là làm đỏ một lượt kéo hoàn toàn đúng.
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);
    $fake->deleteErrorCode = 'ReceiptHandleIsInvalid';

    $transport = sqsTransport($fake);
    $page = $transport->pull('godx.directory.branch', null, 10);

    $transport->ack($page->events[0]->id);
})->throwsNoExceptions();

it('still raises a delete failure that is not an expired handle', function (): void {
    // Chiều ngược: nuốt biên nhận hết hạn KHÔNG được biến thành nuốt mọi lỗi
    // xoá. Quyền IAM thiếu thì message không bao giờ rời hàng đợi, và im lặng ở
    // đó là một vòng lặp vô tận không ai thấy.
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);
    $fake->deleteErrorCode = 'AccessDenied';

    $transport = sqsTransport($fake);
    $page = $transport->pull('godx.directory.branch', null, 10);

    $transport->ack($page->events[0]->id);
})->throws(TransportFailure::class, 'AccessDenied');

// ─── Quyết định 4: dead-letter ─────────────────────────────────────────────

it('quarantines an undecodable body to the dead-letter queue, with the reason attached', function (): void {
    // Thân message không thành envelope ⇒ KHÔNG có event id ⇒ sổ nhận không bao
    // giờ có hàng cho nó ⇒ nó không bao giờ "settled". Để mặc thì nó sang DLQ
    // sau maxReceiveCount lượt mà không mang theo một chữ nào về lý do.
    $fake = (new FakeSqs)->willReceive([sqsMessage('{"not":"an envelope"}', 'rh-bad', 'msg-bad')]);

    $transport = sqsTransport($fake, ['dead_letter_queue_url' => DLQ]);
    $page = $transport->pull('godx.directory.branch', null, 10);

    $sent = $fake->callsOf('SendMessage')[0];

    expect($page->events)->toBe([])
        ->and($sent['QueueUrl'])->toBe(DLQ)
        ->and($sent['MessageBody'])->toBe('{"not":"an envelope"}')
        ->and($sent['MessageAttributes']['QuarantineReason']['StringValue'])->toContain('specversion')
        ->and($fake->callsOf('DeleteMessage')[0]['ReceiptHandle'])->toBe('rh-bad')
        ->and($transport->poisonSeen()[0]['quarantined'])->toBeTrue();
});

it('refuses to delete a poison message when there is nowhere to copy it', function (): void {
    // Xoá một thứ không sao chép được đi đâu là mất dữ liệu do chính ta gây ra,
    // để đổi lấy một hàng đợi sạch mắt. Redrive policy vẫn xử lý được nó.
    $fake = (new FakeSqs)->willReceive([sqsMessage('not json at all', 'rh-bad')]);

    $transport = sqsTransport($fake);
    $transport->pull('godx.directory.branch', null, 10);

    expect($fake->countOf('DeleteMessage'))->toBe(0)
        ->and($fake->countOf('SendMessage'))->toBe(0)
        ->and($transport->poisonSeen()[0]['quarantined'])->toBeFalse()
        ->and($transport->poisonSeen()[0]['message_id'])->toBe('msg-1');
});

it('lets the rest of the batch through when one envelope is poison', function (): void {
    // Một envelope rác nằm giữa các envelope tốt mà làm hỏng cả lượt kéo thì nó
    // chặn đúng thứ nó không liên quan.
    $fake = (new FakeSqs)->willReceive([
        sqsMessage(sqsEnvelope('br_1', 5), 'rh-1', 'msg-1'),
        sqsMessage('{"specversion":"0.3"}', 'rh-bad', 'msg-bad'),
        sqsMessage(sqsEnvelope('br_2', 6), 'rh-2', 'msg-2'),
    ]);

    $page = sqsTransport($fake, ['dead_letter_queue_url' => DLQ])->pull('godx.directory.branch', null, 10);

    expect($page->events)->toHaveCount(2)
        ->and($page->events[0]->resourceId())->toBe('br_1')
        ->and($page->events[1]->resourceId())->toBe('br_2');
});

// ─── Một hàng đợi chở mọi loại tài nguyên ──────────────────────────────────

it('holds a foreign resource type in memory instead of re-hiding it', function (): void {
    // Đẩy về bằng ChangeMessageVisibility(0) cộng 1 vào ApproximateReceiveCount
    // cho mỗi loại tài nguyên mỗi lượt cron — với maxReceiveCount 5, hai lượt
    // cron là đủ để đẩy dữ liệu HOÀN TOÀN LÀNH vào DLQ.
    $fake = (new FakeSqs)->willReceive([
        sqsMessage(sqsEnvelope('br_1', 5), 'rh-branch', 'msg-branch'),
        sqsMessage(sqsEnvelope('org_1', 9, 'godx.directory.organization'), 'rh-org', 'msg-org'),
    ]);

    $transport = sqsTransport($fake);

    $branches = $transport->pull('godx.directory.branch', null, 10);
    $orgs = $transport->pull('godx.directory.organization', null, 10);

    expect($branches->events)->toHaveCount(1)
        ->and($orgs->events)->toHaveCount(1)
        ->and($orgs->events[0]->resourceId())->toBe('org_1')
        // Hàng đợi giả chỉ trả message MỘT lần: lượt nhận thứ hai về rỗng. Envelope
        // organization vẫn tới đích ⇒ nó được GIỮ trong bộ nhớ, không bị đẩy về hàng
        // đợi và cũng không bị vứt đi.
        ->and($fake->countOf('ChangeMessageVisibility'))->toBe(0);
});

it('can still delete a stashed message by its own receipt handle', function (): void {
    $fake = (new FakeSqs)->willReceive([
        sqsMessage(sqsEnvelope('org_1', 9, 'godx.directory.organization'), 'rh-org', 'msg-org'),
    ]);

    $transport = sqsTransport($fake);
    $transport->pull('godx.directory.branch', null, 10);
    $orgs = $transport->pull('godx.directory.organization', null, 10);
    $transport->ack($orgs->events[0]->id);

    expect($fake->callsOf('DeleteMessage')[0]['ReceiptHandle'])->toBe('rh-org');
});

it('routes a resource type that has its own queue to that queue', function (): void {
    $fake = new FakeSqs;

    sqsTransport($fake, ['queues' => ['godx.directory.branch' => 'https://sqs/branches']])
        ->pull('godx.directory.branch', null, 10);

    expect($fake->callsOf('ReceiveMessage')[0]['QueueUrl'])->toBe('https://sqs/branches');
});

// ─── hasMore ───────────────────────────────────────────────────────────────

it('says there is more when the batch came back full', function (): void {
    // SQS không có `has_more` để khai — `ApproximateNumberOfMessages` là một
    // lượt gọi khác và cái tên đã nói nó xấp xỉ. Suy ra "trang đầy = còn nữa"
    // tốn nhiều nhất một lượt long-poll rỗng, mà đó là hình dạng bình thường
    // của việc chờ trên hàng đợi.
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    expect(sqsTransport($fake)->pull('godx.directory.branch', null, 1)->hasMore)->toBeTrue();
});

it('says there is no more when the batch came back short', function (): void {
    $fake = (new FakeSqs)->willReceive([sqsMessage(sqsEnvelope())]);

    expect(sqsTransport($fake)->pull('godx.directory.branch', null, 10)->hasMore)->toBeFalse();
});

it('says there is more while foreign types are still held in memory', function (): void {
    $fake = (new FakeSqs)->willReceive([
        sqsMessage(sqsEnvelope('br_1', 5), 'rh-1', 'msg-1'),
        sqsMessage(sqsEnvelope('br_2', 6), 'rh-2', 'msg-2'),
    ]);

    $transport = sqsTransport($fake);
    $transport->pull('godx.directory.organization', null, 10);

    // Hai envelope branch đang nằm trong bộ nhớ; lượt hỏi branch phải biết là
    // còn hàng, nếu không `FeedPuller` dừng và chúng chỉ tới đích ở lượt cron
    // sau — với visibility timeout đã hết hạn giữa chừng.
    expect($transport->pull('godx.directory.branch', null, 1)->hasMore)->toBeTrue();
});

// ─── Cấu hình thiếu ────────────────────────────────────────────────────────

it('names the missing configuration key instead of calling AWS with an empty queue url', function (): void {
    (new SqsTransport((new FakeSqs)->client, []))->pull('godx.directory.branch', null, 10);
})->throws(TransportFailure::class, 'PLATFORM_SYNC_SQS_QUEUE_URL');

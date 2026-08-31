# platform-laravel-sync

Đồng bộ tài nguyên từ Platform (`id.godx.jp`) xuống mọi consumer Laravel:
envelope CloudEvents 1.0, transport cắm được như driver, sổ nhận chống trùng và
giữ thứ tự, chế độ **shadow** và đối soát phát hiện lệch.

`authz` chỉ là vài loại tài nguyên trong đó. Package này **không** sinh ra để
đồng bộ quyền — nó sinh ra để đồng bộ *tài nguyên*, và permission/role/binding
đi chung đường ống với organization/brand/branch/employee vì chúng giống nhau ở
mọi điểm quan trọng: có id, có phiên bản, có chủ sở hữu.

## Hai chân, và vì sao phải có cả hai

```
      ┌──── LUỒNG EVENT ────────────┐        đường NHANH
      │  sync:pull → inbox → chiếu  │        (trễ một chu kỳ)
      └─────────────────────────────┘
      ┌──── ĐỐI SOÁT ───────────────┐        đường ĐÚNG
      │  sync:reconcile → drift     │        (bắt cái luồng event đánh rơi)
      └─────────────────────────────┘
```

Một hệ chỉ có luồng event thì đúng cho tới lần rơi gói đầu tiên, và sai vĩnh
viễn sau đó, không tiếng động. Chỉ phép **liệt kê đầy đủ** mới trả lời được câu
"tôi có đang thiếu thứ gì mà tôi không biết là mình thiếu không".

## Đường đi của một envelope

```
transport
    │
    ▼
1. loại có đăng ký không?      không → hỏng TO TIẾNG
2. giành event id (INSERT)     thua  → Duplicate
3. đủ trường bắt buộc?         thiếu → Rejected   (vị trí KHÔNG tiến)
4. subject ↔ data.id khớp?     không → Rejected   (xem ngay dưới)
5. sequence có tiến không?     không → Stale      (chặn ghi đè bằng dữ liệu cũ)
6. có khe hở prevsequence?     có    → ghi nhận, KHÔNG chặn hàng đợi
7. shadow hay live?
       shadow → so sánh, ghi báo cáo lệch, KHÔNG ghi bảng
       live   → projector->apply(), rồi mới đẩy vị trí
8. transport là hàng đợi?      ack() — CHỈ khi kết cục đã "settled"
```

Cửa 8 chỉ tồn tại cho transport khai `AcknowledgesDelivery` (SQS). Với feed HTTP,
"đã xong" nói bằng cách đẩy con trỏ, và con trỏ nằm trong tay consumer.

Cửa 3 đứng trước cửa 5 vì payload rác phải bị từ chối kể cả khi nó mang sequence
mới nhất — nếu không, một thay đổi lược đồ phía Platform vừa làm rỗng dữ liệu
vừa đẩy vị trí lên, khiến bản sửa gửi sau bị coi là cũ.

Cửa 4 giữ một bất biến mà cả hệ đứng trên: sổ nhận, `applied_sequence` và báo
cáo lệch khoá theo `subject`, còn projector của consumer viết theo `data['id']`.
Hai giá trị lệch nhau thì phép chống-ghi-đè canh MỘT tài nguyên trong khi phép
ghi rơi vào tài nguyên KHÁC — không tầng nào ném lỗi, và cả hai hàng trông bình
thường sau đó. Chỉ kiểm khi payload có khoá `id`: không phải loại nào cũng đặt
danh tính vào `data`.

## Nối một consumer

```php
// AppServiceProvider::boot()
app(SyncRegistry::class)
    ->resource(DirectoryResources::BRANCH)
    ->projector(BranchProjector::class);
```

```php
final class BranchProjector implements Projector
{
    public function __construct(private readonly BranchService $branches) {}

    public function current(string $resourceId): ?array { /* trạng thái ĐÃ CHUẨN HOÁ */ }
    public function apply(CloudEvent $event): void      { /* PHẢI idempotent  */ }
    public function localIds(): iterable                 { /* để soát chiều ngược */ }
}
```

`current()` phải trả về **cùng khoá, cùng kiểu** với `data` của envelope — shadow
và đối soát so trực tiếp hai mảng này. Trả cột DB thô sẽ làm mọi tài nguyên
trông như đang lệch.

So sánh chỉ chạy trên **phần giao** hai bên: cột riêng của consumer bị bỏ qua,
và trường Platform gửi thêm mà consumer không mirror cũng vậy — trường thứ hai
không nằm trong tay consumer, nên tính nó là lệch thì Platform thêm một cột là
đủ để báo cáo đỏ vĩnh viễn, và một báo cáo luôn đỏ thì không ai đọc. Giao **rỗng**
thì ngược lại: đó là `current()` sai lược đồ, và nó được báo là lệch.

### Trường mà THỨ TỰ không mang nghĩa

Phép so mặc định **nhạy thứ tự**, và mặc định đó đúng: `[a, b]` khác `[b, a]` ở
thứ tự hiển thị, thứ tự ưu tiên, một chuỗi sự kiện. Nhưng có trường mà hai bên
chỉ tình cờ liệt kê khác nhau — tập permission của một vai là ví dụ đã đo được:
consumer sắp nó cho dễ đọc, Platform trả lại đúng thứ tự nó nhận, và **mọi** vai
thành `field_mismatch` vĩnh viễn.

```php
app(SyncRegistry::class)
    ->resource('godx.authz.role')
    ->unordered(['permissions']);
```

Bỏ **thứ tự**, giữ **số lần**: `[a, a, b]` vẫn khác `[a, b]`, vì một phần tử bị
lặp là dữ liệu hỏng chứ không phải cách liệt kê khác. Khai cho một trường không
phải mảng thì rơi về phép so thường — không âm thầm thành "bằng nhau".

Package **cố ý không ship projector nào**. Mỗi consumer có lược đồ riêng; một
projector "dùng chung" là đoán lược đồ của người khác rồi ghi đè dữ liệu đang
chạy.

## Bật một loại lên live

Mặc định mọi loại là `shadow`. Trình tự:

```sh
php artisan sync:pull --type=godx.directory.branch     # chạy shadow vài ngày
php artisan sync:status                                 # xem sổ nhận
php artisan sync:reconcile --type=godx.directory.branch # đọc báo cáo lệch
# lệch đã hiểu và ổn định → mới sửa config:
#   'modes' => ['godx.directory.branch' => 'live'],
```

Đừng bỏ bước shadow. Projector ghi vào chính những bảng đang phục vụ khách, và
một projector sai **không ném lỗi** — nó ghi giá trị sai rồi mọi phép kiểm vẫn
xanh.

## Lệnh

| lệnh | vai | mã thoát |
|---|---|---|
| `sync:pull` | kéo feed, chạy qua sổ nhận | `1` nếu có envelope bị từ chối/hỏng |
| `sync:reconcile` | liệt kê + so sánh; `--repair` mới ghi | `2` khi có lệch · `1` khi lượt đọc **dở dang**, hoặc khi transport không chụp được |
| `sync:status` | loại, chế độ, con trỏ, kết cục | `0` |

`sync:reconcile --repair` bị **từ chối** khi loại còn ở shadow, và nó nói ra —
người vận hành vừa gõ `--repair` và sẽ tin rằng nó đã chạy.

`sync:reconcile` có trần số trang (`--max-pages`, mặc định
`platform-sync.reconcile.max_pages`): điều kiện dừng của vòng đọc ảnh chụp là
`has_more` của **Platform**, nên không trần nghĩa là bên kia quyết định lúc nào
tiến trình của bạn dừng. Chạm trần thì lượt đó **tự khai là chưa đầy đủ**, bỏ
hẳn chiều `orphan_local`, và **không bao giờ thoát 0** — phép trừ tập hợp trên
nửa ảnh chụp là báo động giả hàng loạt, mà `--repair` ở lượt sau xoá theo.

## Transport

**`sqs` là mặc định**, vì **ADR 0002** (`godx-jp/godx-tempo`,
`docs/decisions/0002-platform-tempo-identity-sync.md`, Accepted 2026-08-17) chốt đúng hình dạng đó: transactional outbox trên
Platform → SNS fanout → **một hàng đợi SQS cho mỗi consumer**, kèm DLQ. Package
này ra đời **trước** khi ai đọc bản ghi ấy, với `poll` làm mặc định — đó là lý do
duy nhất `poll` từng đứng ở vị trí này.

**`poll` là PHƯƠNG ÁN LÙI, không phải lựa chọn ngang hàng.** ADR 0002 xét
"delta feed có cursor (Tempo kéo một endpoint)" ở mục *Alternatives considered*
rồi **loại**: đúng về scale, rẻ hơn hẳn, nhưng độ trễ sàn bằng chu kỳ poll và
không đạt yêu cầu "chuẩn quốc tế" mà chủ dự án đã chốt. Dùng nó khi — và chỉ khi:

1. consumer nằm sau NAT / không có đường ra AWS messaging, hoặc
2. bước 3 của lộ trình ADR (Terraform + relay + consumer) chưa xong.

Cả hai đường dùng **chung** bảng outbox ở bước 1, nên bước 1 không phí trong bất
kỳ kịch bản nào.

`aws/aws-sdk-php` nằm ở `require`, không phải `suggest`: nó là phụ thuộc của
driver **mặc định**. Để nó tuỳ chọn thì một consumer cài xong, chạy cron, và đọc
`Class "Aws\Sqs\SqsClient" not found` trong log lúc 3 giờ sáng — một phụ thuộc
bắt buộc mà giả vờ tuỳ chọn chỉ dời lỗi từ lúc `composer install` sang lúc chạy.

Thêm driver mà không sửa package:

```php
app(TransportManager::class)->extend('rabbitmq', fn (array $config) => new AmqpTransport($config));
```

Driver khai **năng lực**, không khai một mặt phẳng chung: `PullsChanges`,
`FetchesResource`, `SnapshotsResource`, `AcknowledgesDelivery`. Lệnh nào cần năng
lực nào thì kiểm và báo bằng tiếng người — thay vì để ba phần tư phương thức ném
`BadMethodCallException` lúc chạy thật.

| | `sqs` | `poll` |
|---|---|---|
| `PullsChanges` | ✅ | ✅ |
| `AcknowledgesDelivery` | ✅ | — (con trỏ tiến LÀ lời "đã xong") |
| `SnapshotsResource` | ❌ **không thể** | ✅ |
| `FetchesResource` | ❌ **không thể** | ✅ |

### Bốn chỗ SQS khác hẳn một feed HTTP

**1. Không có con trỏ.** `ChangePage::$cursor` là `null`, và `pull()` **bỏ qua**
con trỏ nó nhận được. Vị trí của một consumer SQS không phải giá trị nó lưu — nó
là trạng thái của chính hàng đợi: message đã xoá thì không quay lại, chưa xoá thì
sẽ quay lại. Bịa ra một chuỗi (message id cuối, timestamp) rồi cất vào
`platform_sync_cursors` chỉ tạo ra một con số **trông như** vị trí mà không tầng
nào đọc.

⚠️ `null` ở đây **không** có nghĩa "quay về đầu feed" — với `poll` thì nó có
nghĩa đó, và chính vì vậy `PollTransport` cẩn thận trả lại con trỏ **cũ** khi gặp
304. Hai nghĩa trái ngược sống chung được **chỉ vì** `CursorStore` khoá theo
`(transport, loại tài nguyên)`. Đừng gộp khoá đó lại.

**2. Xoá là bước RIÊNG.** `pull()` không xoá gì. `FeedPuller` gọi
`AcknowledgesDelivery::ack()` **sau** khi sổ nhận đã có kết cục cho envelope đó.
Luật là `Verdict::settled()`, không phải "không có lỗi":

| kết cục | xoá? | vì sao |
|---|---|---|
| `applied` · `shadowed` · `gap_noted` · `duplicate` · `stale` | ✅ | đã có kết cục bền trong DB |
| `rejected` | ✅ | sai lược đồ thì lần giao sau sai y hệt; sổ nhận đã giữ nguyên văn nó cùng lý do |
| `failed` | ❌ | projector đổ có thể là sự cố nhất thời — để hàng đợi thử lại rồi dead-letter |
| `claimed` | ❌ | chưa có kết cục nào cả |

Xoá **trong** `pull()` thì một lần chết giữa lúc chiếu làm event bốc hơi vĩnh
viễn. Xoá **sau** thì một lần chết chỉ dẫn tới giao lần hai, và lưới chống trùng
nuốt gọn. Chọn hướng hỏng sửa được.

**3. Visibility timeout.** Xử lý lâu hơn timeout ⇒ message hiện lại và được giao
lần hai **trong khi lượt đầu vẫn đang chạy**. Điều đó **an toàn, và cố ý không
chống**: `InboxStore::claim()` là một INSERT có khoá chính trên `event_id`, chạy
**trước** khi projector chạy, nên đúng một tiến trình thắng còn tiến trình kia
nhận `duplicate`. Đừng dựng heartbeat kéo dài visibility — nó là độ phức tạp cho
một ca đã được phủ.

Hệ quả phải nuốt, không phải sửa: bản thứ hai xoá message bằng biên nhận **mới**
của nó, nên bản thứ nhất xong sau sẽ cầm một biên nhận đã chết. `ack()` bỏ qua
`ReceiptHandleIsInvalid` — nhưng **vẫn ném** mọi lỗi xoá khác (`AccessDenied` là
ca thật: message không bao giờ rời hàng đợi, và im lặng ở đó là một vòng lặp vô
tận không ai thấy).

**4. Dead-letter — hai loại hỏng, hai đường ra.**

- *Projector đổ* (`failed`): envelope hợp lệ, sổ nhận có hàng, lý do đã ghi.
  Driver **không xoá**; redrive policy của SQS đưa nó sang DLQ sau
  `maxReceiveCount` lượt. Không có dòng mã nào cho đường này — đừng dựng lại thứ
  hàng đợi đã có.
- *Thân message không dựng nổi thành envelope*: **không có event id**, nên sổ
  nhận không bao giờ có hàng và nó không bao giờ "settled". Để mặc thì nó sang
  DLQ mà **không mang theo một chữ nào về lý do**. Nên driver tự cách ly ngay:
  `SendMessage` sang `dead_letter_queue_url` kèm `QuarantineReason`, rồi xoá khỏi
  queue chính. **Không khai DLQ thì không xoá** — xoá một thứ không sao chép được
  đi đâu là mất dữ liệu do chính ta gây ra, để đổi lấy một hàng đợi sạch mắt.

Một message hỏng **không** chặn message sau: queue chuẩn (không FIFO) không bảo
toàn thứ tự, và ADR 0002 đã ghi "thứ tự không được giả định" làm bất biến.

### Một hàng đợi chở mọi loại tài nguyên

SNS fanout đổ tất cả vào cùng một queue, trong khi `sync:pull` hỏi theo **từng**
loại. Nên `resourceType` với driver này là **bộ lọc**, không phải khoá định
tuyến: envelope thuộc loại khác được **giữ trong bộ nhớ** cho lượt hỏi loại đó,
chứ không bị xoá và cũng không bị đẩy về bằng `ChangeMessageVisibility(0)`.

Vì sao không đẩy về: mỗi lượt nhận cộng 1 vào `ApproximateReceiveCount`, mà
`maxReceiveCount` là thứ quyết định message nào rơi xuống DLQ. Bốn loại tài
nguyên × mỗi lượt cron = bốn lượt nhận cho một message **hoàn toàn lành**; với
`maxReceiveCount: 5`, hai lượt cron là đủ đẩy dữ liệu đúng vào dead-letter.

Muốn tránh hẳn thì cấu hình ở **phía AWS**, không phải ở đây: filter policy trên
subscription SNS, hoặc một queue cho mỗi loại khai ở `transports.sqs.queues`.

### Đối soát vẫn đi qua HTTP

SQS **không liệt kê được trạng thái hiện tại** — một hàng đợi chỉ đưa cho bạn thứ
vừa thay đổi. Mà `sync:reconcile` đứng trên đúng phép liệt kê ấy. Nên hình dạng
vận hành là **sự kiện qua SQS, ảnh chụp qua HTTP**:

```php
// config/platform-sync.php
'default' => 'sqs',
'reconcile' => ['transport' => 'poll'],
```

hoặc `sync:reconcile --transport=poll` từng lượt. Gõ `sync:reconcile` trên một hệ
chạy SQS mà quên cả hai thì lệnh **dừng kèm một câu giải thích** nêu tên các
transport chụp được — không phải một stack trace. (Bắt gõ `--transport=` mỗi lần
nghĩa là một job có lịch quên gõ sẽ đỏ mãi mãi, và cách sửa nhanh nhất khi đó là
gỡ luôn job đối soát — tức gỡ đúng chân duy nhất bắt được event bị mất hẳn.)

Lộ trình ADR đặt đối soát ở **bước 2**, trước transport ở bước 3, đúng vì lý do
này: nó là phép đo chứng minh bước 3 hoạt động.

## Cái Platform phải cung cấp

Theo ADR 0002, phần lớn khối lượng nằm ở repo Platform (`dxs-platform/platform`),
không ở đây. Tempo là **bên nghe**.

### Đường mặc định (SQS)

1. **Transactional outbox** — bảng `identity_outbox`, ghi trong **cùng
   transaction** với thay đổi danh mục. Thiếu cái này thì mọi thứ còn lại là
   trang trí: "ghi DB xong" và "phát event xong" không nguyên tử, và crash giữa
   hai bước làm mất event hoặc phát event cho một thay đổi đã rollback.
   ⚠️ Móc vào **Eloquent observer**, không phải controller — hai cây controller
   cộng một đường ghi không-HTTP (seeder, tinker trong CD) thì móc ở controller
   là chắc chắn sót, và sót thì im lặng.
2. **Relay worker** đọc outbox → publish lên **SNS topic** (`ap-northeast-1`).
3. **SQS queue cho mỗi consumer** + **DLQ** (`maxReceiveCount`, retention 14
   ngày), tất cả bằng **Terraform** trong `infra/` của repo Platform — producer
   sở hữu topic; thêm consumer về sau **không sửa gì phía producer**.
   ⚠️ **Cảnh báo trên DLQ là điều kiện bắt buộc trước khi bật luồng thật.** Một
   DLQ không ai nhìn chỉ là chỗ message đi chết yên lặng — nó biến "mất event" từ
   sự cố ồn ào thành sự cố im lặng, đúng loại hỏng mà cả ADR sinh ra để chống.
4. **Bật `RawMessageDelivery`** trên subscription, hoặc chấp nhận phong bì
   `{"Type":"Notification","Message":"..."}` — driver mở được cả hai, nhưng chỉ
   một trong hai là thứ bạn cố ý cấu hình.
5. Cân nhắc **filter policy** để mỗi queue chỉ nhận loại tài nguyên consumer đó
   thật sự chiếu (xem *Một hàng đợi chở mọi loại tài nguyên* ở trên).

### Chung cho cả hai đường

6. Envelope **CloudEvents 1.0**; `id` của nó **là** khoá idempotency.
7. `sequence` **đơn điệu theo subject**, và nên kèm `prevsequence`.
8. `data['id']` phải **khớp** phần id trong `subject`.
9. **Feed `snapshot` có cursor** — chân đối soát, và nó là **HTTP**, kể cả khi
   sự kiện đi qua SQS. Feed này **không** được trả 304: consumer không gửi
   `If-None-Match` ở đó có chủ đích, vì một trang rỗng đọc thành "Platform không
   giữ gì" và biến mọi hàng cục bộ thành mồ côi.
10. Khoá ký + đường xoay khoá (JWKS), không phải một secret dán trong env.

⚠️ **Chưa làm được — nói thẳng:** package này **không xác minh chữ ký nào**.
Không JWKS, không RFC 9421, không `Aws\Sns\MessageValidator`. Thứ duy nhất đứng
giữa "envelope do Platform phát" và "consumer tin nó" là **IAM**: chỉ SNS topic
được ghi vào queue, chỉ consumer được đọc. Kênh SNS→SQS nằm trong mạng AWS nên
lập luận đó là thật — nhưng nó **không** phải điều mục 10 đòi, và nó **không**
phủ chân còn lại của ADR 0002: **SNS → HTTPS trực tiếp** cho CAEP, nơi payload đi
qua Internet công khai và `MessageValidator` là bắt buộc. Chân đó chưa tồn tại ở
đâu trong package này. Chỗ tự nhiên để cắm về sau nằm trong `SqsTransport::decode()`,
ở nhánh mở bao SNS — xem docblock của lớp; đừng cài một lớp verify nửa vời, vì
một lớp trông như đã canh còn tệ hơn không có lớp nào.

### Chỉ cho phương án lùi (poll)

11. Feed `changes` có **cursor đục** + **ETag** (304 khi chưa có gì mới).
12. `resource` tra một tài nguyên; **404 nghĩa là đã xoá**, không phải lỗi —
    consumer **không** thử lại nó (chỉ lỗi mạng, 5xx và 429 mới được thử lại;
    thử lại một câu trả lời hợp lệ là nhân ba tải lên Platform để nghe lại đúng
    cái nó vừa nói).

## Giao hàng

At-least-once, **không** phải exactly-once. Exactly-once không tồn tại trên một
đường mạng; thứ tồn tại là at-least-once cộng một hàm idempotent. Envelope mang
**trạng thái đầy đủ** (event-carried state transfer), không phải delta — nên áp
lại một bản cũ hơn là vô hại, còn áp một bản mới hơn hai lần cũng vậy.

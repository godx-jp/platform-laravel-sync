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
4. sequence có tiến không?     không → Stale      (chặn ghi đè bằng dữ liệu cũ)
5. có khe hở prevsequence?     có    → ghi nhận, KHÔNG chặn hàng đợi
6. shadow hay live?
       shadow → so sánh, ghi báo cáo lệch, KHÔNG ghi bảng
       live   → projector->apply(), rồi mới đẩy vị trí
```

Cửa 3 đứng trước cửa 4 vì payload rác phải bị từ chối kể cả khi nó mang sequence
mới nhất — nếu không, một thay đổi lược đồ phía Platform vừa làm rỗng dữ liệu
vừa đẩy vị trí lên, khiến bản sửa gửi sau bị coi là cũ.

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
| `sync:reconcile` | liệt kê + so sánh; `--repair` mới ghi | `2` khi có lệch |
| `sync:status` | loại, chế độ, con trỏ, kết cục | `0` |

`sync:reconcile --repair` bị **từ chối** khi loại còn ở shadow, và nó nói ra —
người vận hành vừa gõ `--repair` và sẽ tin rằng nó đã chạy.

## Transport

`poll` là mặc định: consumer chủ động kéo. Webhook đẩy đòi consumer có địa chỉ
công khai, điều đó loại thẳng mọi consumer sau NAT.

Thêm driver mà không sửa package:

```php
app(TransportManager::class)->extend('rabbitmq', fn (array $config) => new AmqpTransport($config));
```

Driver khai **năng lực**, không khai một mặt phẳng chung: `PullsChanges`,
`FetchesResource`, `SnapshotsResource`. Lệnh nào cần năng lực nào thì kiểm và
báo bằng tiếng người — thay vì để ba phần tư phương thức ném
`BadMethodCallException` lúc chạy thật.

## Cái Platform phải cung cấp

1. **Transactional outbox** — ghi thay đổi domain và envelope trong CÙNG một
   transaction. Thiếu cái này thì mọi thứ còn lại là trang trí: mất event sẽ xảy
   ra và không ai biết.
2. Feed `changes` có **cursor đục** + **ETag** (304 khi chưa có gì mới).
3. Feed `snapshot` có cursor — chân đối soát.
4. `resource` tra một tài nguyên; **404 nghĩa là đã xoá**, không phải lỗi.
5. `sequence` **đơn điệu theo subject**, và nên kèm `prevsequence`.
6. Khoá ký + đường xoay khoá (JWKS), không phải một secret dán trong env.

## Giao hàng

At-least-once, **không** phải exactly-once. Exactly-once không tồn tại trên một
đường mạng; thứ tồn tại là at-least-once cộng một hàm idempotent. Envelope mang
**trạng thái đầy đủ** (event-carried state transfer), không phải delta — nên áp
lại một bản cũ hơn là vô hại, còn áp một bản mới hơn hai lần cũng vậy.

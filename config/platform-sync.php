<?php

declare(strict_types=1);

return [

    /*
    |---------------------------------------------------------------------------
    | Transport mặc định
    |---------------------------------------------------------------------------
    |
    | `sqs` là mặc định vì ADR 0002 (Accepted 2026-08-17) chốt đúng hình dạng
    | đó: transactional outbox trên Platform → SNS fanout → MỘT hàng đợi SQS cho
    | MỖI consumer, kèm DLQ. Bản ghi đó đang có hiệu lực, và mã mâu thuẫn với nó
    | là lỗi của mã.
    |
    | Package này ra đời TRƯỚC khi ai đọc ADR ấy, với `poll` làm mặc định. Đó là
    | lý do duy nhất `poll` từng đứng ở đây — không phải một phép đo nào.
    |
    */

    'default' => env('PLATFORM_SYNC_TRANSPORT', 'sqs'),

    'transports' => [

        /*
        |-----------------------------------------------------------------------
        | sqs — đường bền của ADR 0002
        |-----------------------------------------------------------------------
        |
        | `queue_url` là hàng đợi CỦA CONSUMER NÀY, do Terraform bên repo
        | Platform (`infra/identity-events`) dựng và sở hữu. Consumer chỉ đăng
        | ký vào một SNS topic đã có; nó không tạo topic, không sửa producer.
        |
        | `queues` khai riêng theo loại tài nguyên khi subscription SNS có filter
        | policy tách sẵn. Bỏ trống thì mọi loại dùng chung `queue_url` — hình
        | dạng mặc định của ADR — và driver tự lọc theo loại trong bộ nhớ.
        |
        | `dead_letter_queue_url` chỉ dùng cho một loại hỏng: thân message KHÔNG
        | dựng nổi thành envelope. Loại hỏng còn lại (projector đổ) đi bằng
        | redrive policy của chính hàng đợi — đừng cấu hình chồng lên nó.
        |
        | ⚠️ DLQ không ai nhìn chỉ là chỗ message đi chết yên lặng. ADR 0002 đặt
        | "cảnh báo trên DLQ" làm điều kiện BẮT BUỘC trước khi bật luồng thật, và
        | điều kiện đó nằm ở hạ tầng, không ở file này.
        |
        | Không khai `key`/`secret` là lựa chọn ĐÚNG trên hạ tầng thật: SDK rơi
        | về IAM role. Chúng có mặt ở đây cho máy dev và cho hàng đợi giả
        | (`endpoint` trỏ ElasticMQ/LocalStack).
        |
        */

        'sqs' => [
            'driver' => 'sqs',
            'queue_url' => env('PLATFORM_SYNC_SQS_QUEUE_URL'),
            'queues' => [
                // 'godx.directory.branch' => env('PLATFORM_SYNC_SQS_BRANCH_QUEUE_URL'),
            ],
            'dead_letter_queue_url' => env('PLATFORM_SYNC_SQS_DLQ_URL'),
            'region' => env('PLATFORM_SYNC_SQS_REGION', env('AWS_DEFAULT_REGION', 'ap-northeast-1')),
            'key' => env('PLATFORM_SYNC_SQS_KEY'),
            'secret' => env('PLATFORM_SYNC_SQS_SECRET'),
            'token' => env('PLATFORM_SYNC_SQS_TOKEN'),
            'endpoint' => env('PLATFORM_SYNC_SQS_ENDPOINT'),

            // Long polling. 0 = short polling, và short polling trả RỖNG kể cả
            // khi hàng đợi có message (nó chỉ hỏi một tập con máy chủ) — một
            // vòng cron gặp thế sẽ kết luận "chưa có gì mới".
            'wait_time_seconds' => (int) env('PLATFORM_SYNC_SQS_WAIT', 20),

            // Phải DÀI hơn một lượt xử lý bình thường. Ngắn hơn thì mọi message
            // được giao hai lần: vẫn đúng (sổ nhận chống trùng), nhưng một nửa
            // lưu lượng thành `duplicate` và mọi con số khó đọc.
            'visibility_timeout' => (int) env('PLATFORM_SYNC_SQS_VISIBILITY', 60),
        ],

        /*
        |-----------------------------------------------------------------------
        | poll — PHƯƠNG ÁN LÙI theo ADR 0002, không phải lựa chọn ngang hàng
        |-----------------------------------------------------------------------
        |
        | ADR 0002 xét "delta feed có cursor (Tempo kéo một endpoint)" ở mục
        | *Alternatives considered* và LOẠI nó: đúng về mặt scale, rẻ hơn hẳn,
        | nhưng độ trễ sàn bằng chu kỳ poll và không đạt yêu cầu "chuẩn quốc tế"
        | mà chủ dự án đã chốt. Nó được giữ lại làm phương án lùi, không hơn.
        |
        | Dùng nó khi — và chỉ khi — một trong hai điều sau đúng:
        |
        |   1. Consumer nằm sau NAT / không có đường ra AWS messaging.
        |   2. Bước 3 của lộ trình ADR (Terraform + relay + consumer) chưa xong:
        |      state backend, account/region, và ai chạy `apply` vẫn còn treo.
        |
        | Cả hai đường dùng CHUNG bảng outbox ở bước 1, nên bước 1 không phí
        | trong bất kỳ kịch bản nào.
        |
        | Nó cũng là chân ĐỐI SOÁT: SQS không liệt kê được trạng thái hiện tại,
        | nên `sync:reconcile` phải chạy qua một transport ảnh chụp — xem
        | `reconcile.transport` ở dưới.
        |
        */

        'poll' => [
            'driver' => 'poll',
            'endpoint' => env('PLATFORM_SYNC_ENDPOINT'),
            'token' => env('PLATFORM_SYNC_TOKEN'),
            'timeout' => (int) env('PLATFORM_SYNC_TIMEOUT', 10),
            'connect_timeout' => (int) env('PLATFORM_SYNC_CONNECT_TIMEOUT', 5),
            'retries' => (int) env('PLATFORM_SYNC_RETRIES', 3),
            'retry_delay_ms' => (int) env('PLATFORM_SYNC_RETRY_DELAY_MS', 200),
        ],

        'array' => [
            'driver' => 'array',
        ],

    ],

    /*
    |---------------------------------------------------------------------------
    | Kết nối DB cho sổ nhận
    |---------------------------------------------------------------------------
    |
    | null = kết nối mặc định của ứng dụng. Sổ nhận PHẢI nằm cùng kết nối với
    | các bảng mà projector ghi: nếu không, "đã áp" và "đã ghi" nằm ở hai
    | transaction khác nhau và một lần chết giữa chừng để lại vị trí nói rằng
    | event đã áp trong khi bảng thật chưa đổi.
    |
    */

    'connection' => env('PLATFORM_SYNC_CONNECTION'),

    /*
    |---------------------------------------------------------------------------
    | Chế độ chiếu, theo từng loại tài nguyên
    |---------------------------------------------------------------------------
    |
    | `shadow` (mặc định) chỉ ĐỌC trạng thái cục bộ và ghi báo cáo lệch.
    | `live` cho phép projector ghi vào bảng thật.
    |
    | Đừng bật `live` cho một loại trước khi đọc báo cáo lệch của nó. Projector
    | ghi vào chính những bảng đang phục vụ khách, và một projector sai KHÔNG
    | ném lỗi — nó ghi giá trị sai rồi mọi phép kiểm vẫn xanh.
    |
    | Ví dụ: 'godx.directory.branch' => 'live',
    |
    */

    'modes' => [
        //
    ],

    'pull' => [
        'page_size' => (int) env('PLATFORM_SYNC_PAGE_SIZE', 200),
        'max_pages' => (int) env('PLATFORM_SYNC_MAX_PAGES', 50),
    ],

    /*
    |---------------------------------------------------------------------------
    | Đối soát
    |---------------------------------------------------------------------------
    |
    | `max_pages` là trần AN TOÀN, không phải hạn ngạch: điều kiện dừng của vòng
    | đọc ảnh chụp là `has_more` của Platform, tức do BÊN KIA quyết định. Trần
    | cao hơn của `pull` vì đối soát đọc TOÀN BỘ tập hợp chứ không đọc phần đuôi
    | mới thay đổi.
    |
    | Chạm trần thì lượt đó tự khai là CHƯA ĐẦY ĐỦ và bỏ hẳn chiều `orphan_local`
    | — phép trừ tập hợp trên nửa ảnh chụp là báo động giả hàng loạt.
    |
    */

    'reconcile' => [
        /*
        | Transport dùng cho ẢNH CHỤP, khi transport mặc định không chụp được.
        |
        | Đây là hệ quả trực tiếp của ADR 0002 chứ không phải một tuỳ chọn cho
        | vui: kiến trúc là "sự kiện qua SQS, ảnh chụp qua HTTP", vì một hàng đợi
        | chỉ đưa cho bạn thứ VỪA THAY ĐỔI — nó không có phép liệt kê nào, và
        | đối soát thì đứng trên đúng phép liệt kê ấy.
        |
        | Bỏ trống thì `sync:reconcile` dùng transport mặc định, và nếu cái đó
        | không chụp được thì nó DỪNG kèm một câu giải thích nêu tên các
        | transport chụp được — chứ không ném stack trace.
        |
        | Lộ trình ADR đặt đối soát ở bước 2, TRƯỚC transport ở bước 3, đúng vì
        | lý do này: nó là phép đo chứng minh bước 3 hoạt động.
        */
        'transport' => env('PLATFORM_SYNC_RECONCILE_TRANSPORT'),

        'page_size' => (int) env('PLATFORM_SYNC_RECONCILE_PAGE_SIZE', 500),
        'max_pages' => (int) env('PLATFORM_SYNC_RECONCILE_MAX_PAGES', 200),
    ],

];

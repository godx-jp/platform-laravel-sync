<?php

declare(strict_types=1);

return [

    /*
    |---------------------------------------------------------------------------
    | Transport mặc định
    |---------------------------------------------------------------------------
    |
    | `poll` là mặc định có chủ đích. Webhook đẩy đòi consumer có địa chỉ công
    | khai mà Platform với tới được — điều đó loại thẳng mọi consumer sau NAT,
    | và bắt Platform giữ danh sách endpoint cùng cơ chế thử lại cho từng nơi.
    | Kéo thì đảo chiều gánh nặng: Platform chỉ phục vụ một feed đọc.
    |
    */

    'default' => env('PLATFORM_SYNC_TRANSPORT', 'poll'),

    'transports' => [

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

];

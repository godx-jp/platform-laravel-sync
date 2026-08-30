<?php

declare(strict_types=1);

namespace Godx\Sync\Contracts;

/**
 * Một transport khai NĂNG LỰC của nó, không khai một mặt phẳng chung.
 *
 * Đây là chỗ dễ sai nhất khi bắt chước Manager pattern của Laravel: `Filesystem`
 * ép mọi driver cùng một mặt phẳng được vì mọi kho lưu trữ đều đọc/ghi/xoá
 * được. Transport thì KHÔNG — một webhook đẩy không "pull" được, một hàng đợi
 * không "snapshot" được. Ép chúng cùng một interface nghĩa là ba phần tư
 * phương thức của mỗi driver ném `BadMethodCallException`, và lỗi đó chỉ hiện
 * lúc chạy thật, ở đúng lúc không ai muốn.
 *
 * Vì thế: `Transport` chỉ mang danh tính. Năng lực khai bằng các interface con,
 * và mỗi lệnh (`sync:pull`, `sync:reconcile`) kiểm `instanceof` rồi báo bằng
 * tiếng người rằng driver đang dùng không làm được việc đó.
 */
interface Transport
{
    public function name(): string;
}

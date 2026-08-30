<?php

declare(strict_types=1);

namespace Godx\Sync\Projection;

enum DriftKind: string
{
    /** Platform có, consumer chưa có. */
    case MissingLocal = 'missing_local';

    /** Cả hai đều có, nhưng khác giá trị ở ít nhất một trường. */
    case FieldMismatch = 'field_mismatch';

    /**
     * Consumer còn giữ, Platform không còn.
     *
     * Đây là loại lệch mà LUỒNG EVENT không bao giờ phát hiện được: nếu chính
     * event xoá là cái bị mất thì không có bản nào tới sau để lộ ra. Nó là lý
     * do đối soát phải liệt kê hai chiều chứ không chỉ duyệt ảnh chụp của
     * Platform.
     */
    case OrphanLocal = 'orphan_local';
}

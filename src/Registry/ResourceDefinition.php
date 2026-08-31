<?php

declare(strict_types=1);

namespace Godx\Sync\Registry;

use Godx\Sync\Contracts\Projector;

final class ResourceDefinition
{
    private int $schemaVersion = 1;

    /** @var class-string<Projector>|null */
    private ?string $projector = null;

    private ProjectionMode $mode = ProjectionMode::Shadow;

    /** @var list<string> */
    private array $requiredFields = [];

    /** @var list<string> */
    private array $unorderedFields = [];

    public function __construct(public readonly string $type) {}

    public function schemaVersion(int $version): self
    {
        $this->schemaVersion = $version;

        return $this;
    }

    /** @param  class-string<Projector>  $class */
    public function projector(string $class): self
    {
        $this->projector = $class;

        return $this;
    }

    /**
     * Trường mà payload BẮT BUỘC phải có.
     *
     * Đây không phải validation đầy đủ — nó là rào tối thiểu chống một thay đổi
     * lược đồ phía Platform lặng lẽ làm rỗng một cột. Payload thiếu trường khai
     * ở đây bị từ chối ở inbox và ghi lại lý do, thay vì đi tới projector rồi
     * ghi null.
     *
     * @param  list<string>  $fields
     */
    public function requires(array $fields): self
    {
        $this->requiredFields = $fields;

        return $this;
    }

    /**
     * Trường mà THỨ TỰ không mang nghĩa — so như một TẬP, không như một danh
     * sách.
     *
     * Mặc định của `DriftRecorder` là nhạy thứ tự, và mặc định đó đúng: ở phần
     * lớn payload, `[a, b]` khác `[b, a]` thật (thứ tự hiển thị, thứ tự ưu
     * tiên, một chuỗi sự kiện). Đổi mặc định là làm hệ mù với cả một lớp lệch.
     *
     * Nhưng có trường mà hai bên chỉ tình cờ liệt kê khác nhau. Ví dụ đã đo:
     * tập permission của một vai. Consumer sắp xếp nó để đọc cho dễ, Platform
     * trả lại đúng thứ tự nó nhận — và phép so nhạy thứ tự biến MỌI vai thành
     * `field_mismatch` vĩnh viễn, đúng cái hình dạng "báo cáo luôn đỏ thì không
     * ai đọc" mà `DriftRecorder` sinh ra để tránh.
     *
     * Khai ở đây là lời khai của CONSUMER về ngữ nghĩa trường của chính nó, nên
     * nó thuộc về định nghĩa tài nguyên chứ không phải một cờ toàn cục.
     *
     * @param  list<string>  $fields
     */
    public function unordered(array $fields): self
    {
        $this->unorderedFields = $fields;

        return $this;
    }

    public function mode(ProjectionMode $mode): self
    {
        $this->mode = $mode;

        return $this;
    }

    public function currentSchemaVersion(): int
    {
        return $this->schemaVersion;
    }

    /** @return class-string<Projector>|null */
    public function projectorClass(): ?string
    {
        return $this->projector;
    }

    public function projectionMode(): ProjectionMode
    {
        return $this->mode;
    }

    /** @return list<string> */
    public function required(): array
    {
        return $this->requiredFields;
    }

    /** @return list<string> */
    public function unorderedFields(): array
    {
        return $this->unorderedFields;
    }
}

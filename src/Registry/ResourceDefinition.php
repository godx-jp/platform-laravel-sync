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
}

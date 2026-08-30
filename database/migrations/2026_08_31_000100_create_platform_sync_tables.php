<?php

declare(strict_types=1);

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Sổ nhận. Khoá chính LÀ id của envelope — chống trùng bằng ràng buộc
        // của DB chứ không bằng một phép kiểm trong PHP: hai worker cùng xử lý
        // một event sẽ có đúng một cái thắng, và cái thua nhận lỗi trùng khoá
        // chứ không cùng nhau ghi hai lần.
        Schema::create('platform_sync_inbox', function (Blueprint $table): void {
            $table->ulid('event_id')->primary();
            $table->string('resource_type', 191);
            $table->string('resource_id', 191);
            $table->string('event_type', 191);
            $table->unsignedBigInteger('sequence');
            $table->unsignedBigInteger('previous_sequence')->nullable();
            $table->string('tenant_id', 191);
            $table->json('payload');
            $table->string('verdict', 32);
            $table->text('note')->nullable();
            $table->timestamp('received_at');
            $table->timestamp('settled_at')->nullable();

            $table->index(['resource_type', 'resource_id', 'sequence'], 'psi_resource_seq_idx');
            $table->index(['resource_type', 'verdict'], 'psi_type_verdict_idx');
            $table->index('received_at', 'psi_received_idx');
        });

        // Vị trí đã áp, theo từng tài nguyên. Tách khỏi sổ nhận có chủ đích: sổ
        // nhận được phép dọn theo thời gian, còn vị trí thì KHÔNG — mất nó là
        // mất khả năng nhận ra event đến muộn, và hệ sẽ lặng lẽ lùi dữ liệu về
        // một trạng thái cũ.
        Schema::create('platform_sync_positions', function (Blueprint $table): void {
            $table->string('resource_type', 191);
            $table->string('resource_id', 191);
            $table->unsignedBigInteger('applied_sequence');
            $table->string('last_event_id', 26);
            $table->timestamp('applied_at');

            $table->primary(['resource_type', 'resource_id'], 'psp_pk');
        });

        // Con trỏ feed, theo từng loại tài nguyên VÀ từng transport: đổi
        // transport là đổi hệ quy chiếu của con trỏ, và dùng lại con trỏ cũ sẽ
        // kéo nhầm đoạn.
        Schema::create('platform_sync_cursors', function (Blueprint $table): void {
            $table->string('transport', 64);
            $table->string('resource_type', 191);
            $table->text('cursor')->nullable();
            $table->timestamp('pulled_at')->nullable();
            $table->unsignedBigInteger('pulled_count')->default(0);

            $table->primary(['transport', 'resource_type'], 'psc_pk');
        });

        // Báo cáo lệch. Đây là SẢN PHẨM của chế độ shadow, không phải log phụ —
        // nên nó có lược đồ, có thể truy vấn, và không bị xoay vòng cùng log
        // ứng dụng.
        Schema::create('platform_sync_drift', function (Blueprint $table): void {
            $table->id();
            $table->ulid('run_id');
            $table->string('resource_type', 191);
            $table->string('resource_id', 191);
            $table->string('kind', 32);
            $table->json('remote')->nullable();
            $table->json('local')->nullable();
            $table->json('differing_fields')->nullable();
            $table->timestamp('observed_at');

            $table->index(['run_id'], 'psd_run_idx');
            $table->index(['resource_type', 'kind'], 'psd_type_kind_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('platform_sync_drift');
        Schema::dropIfExists('platform_sync_cursors');
        Schema::dropIfExists('platform_sync_positions');
        Schema::dropIfExists('platform_sync_inbox');
    }
};

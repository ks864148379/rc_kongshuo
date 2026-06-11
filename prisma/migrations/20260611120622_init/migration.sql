-- CreateTable
CREATE TABLE "notification_task" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "idempotency_key" TEXT NOT NULL,
    "target_url" TEXT NOT NULL,
    "headers" TEXT NOT NULL DEFAULT '{}',
    "body" TEXT,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "attempt_count" INTEGER NOT NULL DEFAULT 0,
    "max_attempts" INTEGER NOT NULL DEFAULT 5,
    "next_attempt_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "last_status_code" INTEGER,
    "last_error" TEXT,
    "created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" DATETIME NOT NULL
);

-- CreateIndex
CREATE UNIQUE INDEX "notification_task_idempotency_key_key" ON "notification_task"("idempotency_key");

-- CreateIndex
CREATE INDEX "idx_recovery" ON "notification_task"("status", "next_attempt_at");

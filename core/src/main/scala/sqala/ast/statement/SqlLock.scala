package sqala.ast.statement

enum SqlLock(val lockMode: String, val waitMode: Option[SqlLockWaitMode]):
    case Update(override val waitMode: Option[SqlLockWaitMode]) extends SqlLock("FOR UPDATE", waitMode)
    case Share(override val waitMode: Option[SqlLockWaitMode]) extends SqlLock("FOR SHARE", waitMode)

enum SqlLockWaitMode(val waitMode: String):
    case NoWait extends SqlLockWaitMode("NOWAIT")
    case SkipLocked extends SqlLockWaitMode("SKIP LOCKED")
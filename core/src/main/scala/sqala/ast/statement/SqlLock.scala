package sqala.ast.statement

enum SqlLock(val waitMode: Option[SqlLockWaitMode]):
    case Update(override val waitMode: Option[SqlLockWaitMode]) extends SqlLock(waitMode)
    case Share(override val waitMode: Option[SqlLockWaitMode]) extends SqlLock(waitMode)

enum SqlLockWaitMode:
    case NoWait
    case SkipLocked
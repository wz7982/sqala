package sqala.ast.statement

/**
 * Row-level locking clause for `SELECT` statements.
 */
enum SqlLock(val waitMode: Option[SqlLockWaitMode]):
    /**
     * Exclusive lock on selected rows.
     *
     * Renders as `FOR UPDATE [NOWAIT|SKIP LOCKED]`.
     */
    case Update(override val waitMode: Option[SqlLockWaitMode]) extends SqlLock(waitMode)

    /**
     * Shared lock on selected rows.
     *
     * Renders as `FOR SHARE [NOWAIT|SKIP LOCKED]`.
     */
    case Share(override val waitMode: Option[SqlLockWaitMode]) extends SqlLock(waitMode)

/**
 * Lock wait mode.
 */
enum SqlLockWaitMode:
    /**
     * Do not wait if rows are locked; fail immediately.
     *
     * Renders as `NOWAIT`.
     */
    case NoWait

    /**
     * Skip locked rows instead of waiting.
     *
     * Renders as `SKIP LOCKED`.
     */
    case SkipLocked
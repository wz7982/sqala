package sqala.jdbc

/**
 * A cursor batch produced by `cursorFetch`, streaming a subset
 * of the query result. `batchNo` is the 1‑based batch index,
 * `batchSize` is the configured fetch size, and `data` is the
 * current batch of decoded rows.
 */
case class Cursor[T](
    batchNo: Int,
    batchSize: Int,
    data: List[T]
)
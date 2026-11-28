package sqala.jdbc

/**
 * A paginated query result. `pageTotal` is the total number of
 * pages, `querySize` is the total row count, `pageSize` is the
 * configured page size, `pageNo` is the current page number
 * (1‑based), and `data` is the current page's rows.
 */
case class Page[T](
    pageTotal: Int,
    querySize: Long,
    pageSize: Int,
    pageNo: Int,
    data: List[T]
):
    /**
     * Transforms the page data with a mapping function.
     *
     * {{{
     * db.page(from(User), 10, 1).map(u => u.name)
     * }}}
     */
    def map[R](f: T => R): Page[R] = copy(data = data.map(f))
package sqala.jdbc

case class Page[T](total: Long, count: Long, current: Long, size: Long, data: List[T]):
    def map[R](f: T => R): Page[R] = copy(data = data.map(f))
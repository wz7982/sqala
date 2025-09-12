package sqala.jdbc

case class Page[T](
    pageTotal: Int, 
    querySize: Long,
    pageSize: Int, 
    pageNo: Int, 
    data: List[T]
):
    def map[R](f: T => R): Page[R] = copy(data = data.map(f))
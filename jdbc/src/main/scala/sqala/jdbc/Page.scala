package sqala.jdbc

case class Page[T](
    pageTotal: Int, 
    querySize: Long, 
    pageNo: Int, 
    pageSize: Int, 
    data: List[T]
):
    def map[R](f: T => R): Page[R] = copy(data = data.map(f))
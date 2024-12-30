package sqala.jdbc

case class Cursor[T](
    batchNo: Int,
    batchSize: Int,
    data: List[T]
)
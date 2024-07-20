package sqala.dsl

case class Table[T](
    private[sqala] __tableName__ : String, 
    private[sqala] __aliasName__ : String,
    private[sqala] __metaData__ : TableMetaData
) extends Selectable:
    type Fields = NamedTuple.Map[NamedTuple.From[Unwrap[T, Option]], [x] =>> MapField[x, T]]

    def selectDynamic(name: String): Expr[?, ?] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Expr.Column(__aliasName__, columnMap(name))

object Table:
    extension [T](table: Table[T])
        def * : table.Fields =
            val columns = table.__metaData__.columnNames
                .map(n => Expr.Column(table.__aliasName__, n))
            val columnTuple = Tuple.fromArray(columns.toArray)
            NamedTuple(columnTuple).asInstanceOf[table.Fields]

case class TableMetaData(
    tableName: String,
    primaryKeyFields: List[String],
    incrementField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)
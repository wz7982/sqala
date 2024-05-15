package sqala.dsl

case class Table[T](
    private[sqala] __tableName__ : String, 
    private[sqala] __aliasName__ : String,
    private[sqala] __metaData__ : TableMetaData
) extends Selectable:
    type Fields = NamedTuple.Map[NamedTuple.From[Unwrap[T, Option]], [x] =>> MapField[x, T]]

    def selectDynamic(name: String): Column[?] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Column(__aliasName__, columnMap(name))

case class TableMetaData(
    tableName: String,
    primaryKeyFields: List[String],
    incrementField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)
package sqala.static.metadata

case class TableMetaData(
    tableName: String,
    primaryKeyFields: List[String],
    incrementField: Option[String],
    columnNames: List[String],
    fieldNames: List[String]
)

private[sqala] val tableCte = "__cte__"

private[sqala] val columnPseudoLevel = "__pseudo__level__"
package sqala.static.metadata

final case class TableMetaData(
    private[sqala] val tableName: String,
    private[sqala] val primaryKeyFields: List[String],
    private[sqala] val incrementField: Option[String],
    private[sqala] val columnNames: List[String],
    private[sqala] val fieldNames: List[String]
)

private[sqala] val tableCte = "__cte__"

private[sqala] val columnPseudoLevel = "__pseudo__level__"
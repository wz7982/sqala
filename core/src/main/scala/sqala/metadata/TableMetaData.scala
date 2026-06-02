package sqala.metadata

/**
 * Metadata extracted from an entity case class, describing its mapping to a database table.
 *
 * @param tableName the database table name.
 * @param primaryKeyFields the field names annotated with `@primaryKey` or `@autoInc`.
 * @param incrementField the field name annotated with `@autoInc`, if any.
 * @param columnNames the database column names corresponding to each field.
 * @param fieldNames the Scala field names of the entity.
 */
private[sqala] final case class TableMetaData(
    private[sqala] val tableName: String,
    private[sqala] val primaryKeyFields: List[String],
    private[sqala] val incrementField: Option[String],
    private[sqala] val columnNames: List[String],
    private[sqala] val fieldNames: List[String]
)
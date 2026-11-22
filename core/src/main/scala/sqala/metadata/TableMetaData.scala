package sqala.metadata

import sqala.util.NonEmptyList

/**
 * Metadata extracted from an entity case class, describing its mapping to a database table.
 */
final case class TableMetaData(
    val tableName: String,
    val primaryKeyFields: List[String],
    val incrementField: Option[String],
    val columns: NonEmptyList[String],
    val fields: NonEmptyList[String]
)
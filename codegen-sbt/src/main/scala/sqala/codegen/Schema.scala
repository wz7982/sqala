package sqala.codegen

final case class ColumnSchema(
    name: String,
    jdbcType: Int,
    typeName: String,
    nullable: Boolean,
    autoIncrement: Boolean,
    generated: Boolean,
    size: Int,
    decimalDigits: Int,
    ordinal: Int
)

final case class TableSchema(
    name: String,
    columns: Seq[ColumnSchema],
    primaryKeys: Set[String]
)

package sqala.codegen

final case class CodegenConfig(
    url: String,
    user: String,
    password: String,
    driver: Option[String],
    schema: Option[String],
    tables: Seq[String],
    outputPackage: String,
    tableNaming: TableNaming,
    typeMappings: Map[String, String]
)

package sqala.json

case class JsonMetaData(
    fieldNames: List[String], 
    aliasNames: List[Option[String]], 
    ignore: List[Boolean],
    defaultValues: List[Option[?]]
)
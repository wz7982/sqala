package sqala.json.annotation

import scala.annotation.StaticAnnotation

@scala.annotation.meta.field
class jsonIgnore() extends StaticAnnotation

@scala.annotation.meta.field
case class jsonAlias(alias: String) extends StaticAnnotation
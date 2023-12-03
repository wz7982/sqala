package sqala.compiletime.annotation

import scala.annotation.StaticAnnotation

@scala.annotation.meta.field
case class table(tableName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class primaryKey(columnName: String = "") extends StaticAnnotation

@scala.annotation.meta.field
case class incrementKey(columnName: String = "") extends StaticAnnotation

@scala.annotation.meta.field
case class column(columnName: String) extends StaticAnnotation
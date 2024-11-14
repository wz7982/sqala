package sqala.dsl.annotation

import scala.annotation.StaticAnnotation

@scala.annotation.meta.field
case class table(tableName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class column(columnName: String) extends StaticAnnotation

@scala.annotation.meta.field
class primaryKey extends StaticAnnotation

@scala.annotation.meta.field
class autoInc extends StaticAnnotation

@scala.annotation.meta.field
class sqlFunction extends StaticAnnotation

@scala.annotation.meta.field
class sqlAgg extends StaticAnnotation

@scala.annotation.meta.field
class sqlWindow extends StaticAnnotation
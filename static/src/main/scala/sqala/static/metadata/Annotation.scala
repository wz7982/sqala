package sqala.static.metadata

import scala.annotation.StaticAnnotation

@scala.annotation.meta.field
case class entityTable(tableName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class column(columnName: String) extends StaticAnnotation

@scala.annotation.meta.field
class primaryKey extends StaticAnnotation

@scala.annotation.meta.field
class autoInc extends StaticAnnotation

@scala.annotation.meta.field
class function extends StaticAnnotation

@scala.annotation.meta.field
class aggFunction extends StaticAnnotation

@scala.annotation.meta.field
class windowFunction extends StaticAnnotation
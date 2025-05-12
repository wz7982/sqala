package sqala.metadata

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
class function(functionName: String) extends StaticAnnotation

@scala.annotation.meta.field
class agg(functionName: String) extends StaticAnnotation

@scala.annotation.meta.field
class window(functionName: String) extends StaticAnnotation
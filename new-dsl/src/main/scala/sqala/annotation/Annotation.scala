package sqala.annotation

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
case class sqlFunction(functionName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class sqlAgg(functionName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class sqlWindow(functionName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class sqlBinaryOperator(operatorName: String) extends StaticAnnotation
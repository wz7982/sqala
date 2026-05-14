package sqala.static.metadata

import scala.annotation.StaticAnnotation

@scala.annotation.meta.field
final class table(tableName: String) extends StaticAnnotation

@scala.annotation.meta.field
final class column(columnName: String) extends StaticAnnotation

@scala.annotation.meta.field
final class primaryKey extends StaticAnnotation

@scala.annotation.meta.field
final class autoInc extends StaticAnnotation

@scala.annotation.meta.field
final class view(prefix: String, key: String) extends StaticAnnotation

@scala.annotation.meta.field
final class derivedField[T, R](source: String, mapper: T => R) extends StaticAnnotation

@scala.annotation.meta.field
final class nested extends StaticAnnotation
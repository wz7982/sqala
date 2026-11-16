package sqala.metadata

import scala.annotation.StaticAnnotation

/**
 * Marks a case class with a custom table name, overriding the default
 * CamelCase-to-snake_case name conversion.
 *
 * {{{
 * @table("USER")
 * case class User(@column("ID") id: Int, name: String)
 * }}}
 */
@scala.annotation.meta.field
final class table(tableName: String) extends StaticAnnotation

/**
 * Marks a field with a custom column name, overriding the default
 * CamelCase-to-snake_case name conversion.
 *
 * {{{
 * @column("ID")
 * id: Int
 * }}}
 */
@scala.annotation.meta.field
final class column(columnName: String) extends StaticAnnotation

/**
 * Marks a field as part of the primary key.
 *
 * Multiple fields can be annotated to form a composite primary key.
 *
 * {{{
 * case class User(@primaryKey id: Int, name: String)
 * }}}
 */
@scala.annotation.meta.field
final class primaryKey extends StaticAnnotation

/**
 * Marks a field as an auto-increment primary key.
 *
 * Auto-increment columns are excluded from `INSERT` statements and are
 * implicitly treated as part of the primary key.
 *
 * {{{
 * case class User(@autoInc id: Int, name: String)
 * }}}
 */
@scala.annotation.meta.field
final class autoInc extends StaticAnnotation
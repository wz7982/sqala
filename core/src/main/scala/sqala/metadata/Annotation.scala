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

/**
 * Declares the database value of an enum case when the enum is mapped
 * to an integer column.
 *
 * Every case of an integer-mapped enum must declare this annotation.
 * sqala never falls back to the ordinal, because reordering cases would
 * silently change stored data.
 *
 * {{{
 * enum DataState:
 *     @enumValue(1) case Active
 *     @enumValue(0) case Deleted
 *
 * object DataState:
 *     given CustomField[DataState, Int] = CustomField.derived
 * }}}
 */
final class enumValue(value: Int) extends StaticAnnotation

/**
 * Declares the database value of an enum case when the enum is mapped
 * to a string column.
 *
 * This annotation is optional: when it is absent, the case name is used.
 * Renaming the case therefore changes the stored value, so use this
 * annotation when the string must stay stable.
 *
 * {{{
 * enum Status:
 *     @enumName("ACTIVE") case Active
 *     case Deleted
 *
 * object Status:
 *     given CustomField[Status, String] = CustomField.derived
 * }}}
 */
final class enumName(value: String) extends StaticAnnotation
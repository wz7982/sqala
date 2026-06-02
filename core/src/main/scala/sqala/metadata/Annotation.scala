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
 *
 * @param tableName the database table name.
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
 *
 * @param columnName the database column name.
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
 * Marks a case class as a view mapping for converting flat query results
 * into a cascade (one-to-many) structure.
 *
 * {{{
 * @view(prefix = "user", key = "id")
 * case class UserVO(
 *     id: Int,
 *     name: String,
 *     @derivedField[Int, Boolean](source = "level", mapper = _ > 5)
 *     vip: Boolean,
 *     @nested
 *     orders: List[OrderVO]
 * )
 * }}}
 *
 * @param prefix the field name prefix used to identify columns belonging
 *               to this view in the flat result set.
 * @param key the primary key field name used to group child rows.
 */
@scala.annotation.meta.field
final class view(prefix: String, key: String) extends StaticAnnotation

/**
 * Marks a view field whose value is derived from a source field via a
 * mapping function, rather than directly from a query column.
 *
 * {{{
 * @derivedField[Int, Boolean](source = "level", mapper = _ > 5)
 * vip: Boolean
 * }}}
 *
 * @tparam T the source field type.
 * @tparam R the derived field type.
 *
 * @param source the source field name in the flat result set.
 * @param mapper a function transforming the source value to the derived value.
 */
@scala.annotation.meta.field
final class derivedField[T, R](source: String, mapper: T => R) extends StaticAnnotation

/**
 * Marks a view field as a nested cascade structure (`List[T]`), populated
 * from child rows grouped by the parent key.
 *
 * {{{
 * @nested
 * orders: List[OrderVO]
 * }}}
 */
@scala.annotation.meta.field
final class nested extends StaticAnnotation
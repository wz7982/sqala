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
 * Marks a case class as a view mapping for converting flat query results
 * into a cascade (one-to-many) structure.
 *
 * {{{
 * @view(prefix = "post", key = "id")
 * case class PostVO(
 *     id: Int,
 *     title: String,
 *     likeCount: Int,
 *     @derivedField[Int, Boolean](source = "likeCount", mapper = _ > 10)
 *     hot: Boolean,
 *     @nested
 *     comments: List[CommentVO]
 * )
 * }}}
 */
@scala.annotation.meta.field
final class view(prefix: String, key: String) extends StaticAnnotation

/**
 * Marks a view field whose value is derived from a source field via a
 * mapping function, rather than directly from a query column.
 *
 * {{{
 * @derivedField[Int, Boolean](source = "likeCount", mapper = _ > 10)
 * hot: Boolean
 * }}}
 */
@scala.annotation.meta.field
final class derivedField[T, R](source: String, mapper: T => R) extends StaticAnnotation

/**
 * Marks a view field as a nested cascade structure (`List[T]`), populated
 * from child rows grouped by the parent key.
 *
 * {{{
 * @nested
 * comments: List[CommentVO]
 * }}}
 */
@scala.annotation.meta.field
final class nested extends StaticAnnotation
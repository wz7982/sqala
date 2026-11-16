package sqala.dynamic.native

import scala.collection.mutable.ArrayBuffer

/**
 * A raw SQL fragment together with its positional `?` parameters.
 *
 * Instances are normally created with the `sql` string interpolator,
 * which replaces each interpolated argument with a `?` placeholder and
 * collects the arguments separately. Because values are always bound as
 * parameters, user input is not spliced into the SQL text.
 *
 * {{{
 * val query = sql"SELECT * FROM user WHERE id = $id"
 * }}}
 */
final class NativeSql(val sql: String, val args: Array[Any]):
    /**
     * Concatenates another fragment. The SQL texts are joined and the
     * parameter arrays are appended in order.
     *
     * {{{
     * sql"SELECT * FROM user WHERE 1 = 1" + sql" AND id = $id"
     * }}}
     */
    def +(that: NativeSql): NativeSql =
        NativeSql(sql + that.sql, args.appendedAll(that.args))

    /**
     * Appends a raw SQL string, keeping the current parameters unchanged.
     *
     * {{{
     * sql"SELECT * FROM user WHERE id = $id" + " ORDER BY id"
     * }}}
     */
    def +(s: String): NativeSql =
        NativeSql(sql + s, args)

extension (s: StringContext)
    /**
     * Builds a NativeSql from an interpolated string, turning each
     * argument into a positional `?` placeholder.
     *
     * A `List` argument is expanded into a parenthesized placeholder
     * list, so it can be used directly in an `IN` clause. All other
     * arguments become a single placeholder.
     *
     * {{{
     * val id = 1
     * sql"SELECT * FROM user WHERE id = $id"
     *
     * val ids = List(1, 2, 3)
     * sql"SELECT * FROM user WHERE id IN $ids"
     * // renders as: SELECT * FROM user WHERE id IN (?, ?, ?)
     * }}}
     */
    inline def sql(inline args: Any*): NativeSql =
        val strings = s.parts.iterator
        val argArray = ArrayBuffer[Any]()
        val argIterator = args.iterator
        val builder = new StringBuilder(strings.next())
        while strings.hasNext do
            if argIterator.hasNext then
                val arg = argIterator.next()
                arg match
                    case l: List[_] =>
                        builder.append(l.map(_ => "?").mkString("(", ", ", ")"))
                        argArray.appendAll(l)
                    case _ =>
                        builder.append("?")
                        argArray.append(arg)
            builder.append(strings.next())
        NativeSql(builder.toString, argArray.toArray)
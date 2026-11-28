package sqala.jdbc

import scala.collection.mutable.ListBuffer

/**
 * A raw SQL fragment together with its positional `?` arguments.
 *
 * Instances are normally created with the `sql` string interpolator,
 * which replaces each interpolated argument with a `?` placeholder and
 * collects a typed `JdbcArg` for it. Because values are always bound as
 * parameters, user input is not spliced into the SQL text.
 *
 * {{{
 * val query = sql"SELECT * FROM user WHERE id = $id"
 * }}}
 */
final class NativeSql(
    val sql: String,
    val args: Array[JdbcArg[?]]
):
    /**
     * Concatenates another fragment. The SQL texts are joined and the
     * argument arrays are appended in order.
     *
     * {{{
     * sql"SELECT * FROM user WHERE 1 = 1" + sql" AND id = $id"
     * }}}
     */
    def +(that: NativeSql): NativeSql =
        new NativeSql(sql + that.sql, args.appendedAll(that.args))

    /**
     * Appends a raw SQL string, keeping the current arguments unchanged.
     *
     * {{{
     * sql"SELECT * FROM user WHERE id = $id" + " ORDER BY id"
     * }}}
     */
    def +(s: String): NativeSql =
        new NativeSql(sql + s, args)

object NativeSql:
    /**
     * Builds a `NativeSql` from a `StringContext` and encoded arguments.
     *
     * `Left` represents a single `?` placeholder; `Right` represents a
     * sequence expanded into a parenthesized placeholder list.
     *
     * This method is used by the `sql` interpolator and is public only
     * because macro expansions run in user code.
     */
    def build(
        sc: StringContext,
        values: List[Either[JdbcArg[?], Seq[JdbcArg[?]]]]
    ): NativeSql =
        val parts = sc.parts
        val sqlBuilder = new StringBuilder()
        val argsBuffer = ListBuffer[JdbcArg[?]]()
        parts.zipWithIndex.foreach: (part, index) =>
            sqlBuilder.append(part)
            if index < values.length then
                values(index) match
                    case Left(arg) =>
                        sqlBuilder.append("?")
                        argsBuffer.addOne(arg)
                    case Right(seq) if seq.isEmpty =>
                        throw new IllegalArgumentException(
                            "The sql interpolator cannot expand an empty Seq. Use a non-empty Seq or omit the placeholder."
                        )
                    case Right(seq) =>
                        sqlBuilder.append(seq.map(_ => "?").mkString("(", ", ", ")"))
                        argsBuffer.addAll(seq)
        new NativeSql(sqlBuilder.toString, argsBuffer.toArray)

extension (sc: StringContext)
    /**
     * Builds a `NativeSql` from an interpolated string, turning each
     * argument into a positional `?` placeholder.
     *
     * A `Seq` argument is expanded into a parenthesized placeholder
     * list, so it can be used directly in an `IN` clause. All other
     * arguments become a single placeholder. An empty `Seq` is rejected
     * because it would produce an invalid empty placeholder list.
     *
     * Each argument must have a `JdbcEncoder` instance. The interpolator
     * selects it at compile time and stores the typed `JdbcArg` alongside
     * the SQL text.
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
        ${ NativeSqlMacro.build('sc, 'args) }
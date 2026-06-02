package sqala.util

import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.metadata.Dialect

/**
 * Recursively converts a CamelCase character list to snake_case.
 *
 * @param s the character list of the CamelCase string.
 */
private[sqala] def camelListToSnakeList(s: List[Char]): List[Char] = 
    s match
        case x :: y :: t if y.isUpper => 
            x.toLower :: '_' :: camelListToSnakeList(y :: t)
        case h :: t => 
            h.toLower :: camelListToSnakeList(t)
        case Nil => 
            Nil

/**
 * Converts a CamelCase string to snake_case.
 *
 * {{{
 * camelToSnake("userId") // "user_id"
 * }}}
 *
 * @param s the CamelCase string.
 */
private[sqala] def camelToSnake(s: String): String =
    camelListToSnakeList(s.toList).mkString

/**
 * Pipe-forward operator, enabling left-to-right data flow style.
 *
 * {{{
 * 1 |> print
 * }}}
 *
 * @tparam A the input type.
 * @tparam B the output type.
 */
extension [A, B](a: A)
    private[sqala] def |>(f: A => B): B = f(a)

/**
 * Converts a query AST to a dialect-specific SQL string.
 *
 * @param query the query AST node.
 * @param dialect the target database dialect.
 * @param standardEscapeStrings `true` treats backslashes literally (standard
 *                              behavior, e.g. PostgreSQL, Oracle); `false`
 *                              uses backslashes as escape characters
 *                              (e.g. MySQL).
 */
def queryToString(
    query: SqlQuery,
    dialect: Dialect,
    standardEscapeStrings: Boolean
): String =
    val printer = dialect.printer(standardEscapeStrings)
    printer.printQuery(query)
    printer.sql

/**
 * Converts a statement AST to a dialect-specific SQL string.
 *
 * @param statement the statement AST node.
 * @param dialect the target database dialect.
 * @param standardEscapeStrings `true` treats backslashes literally (standard
 *                              behavior, e.g. PostgreSQL, Oracle); `false`
 *                              uses backslashes as escape characters
 *                              (e.g. MySQL).
 */
def statementToString(
    statement: SqlStatement,
    dialect: Dialect,
    standardEscapeStrings: Boolean
): String =
    val printer = dialect.printer(standardEscapeStrings)
    printer.printStatement(statement)
    printer.sql
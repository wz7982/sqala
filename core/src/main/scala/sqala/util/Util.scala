package sqala.util

import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.printer.Dialect

private[sqala] def camelListToSnakeList(s: List[Char]): List[Char] = s match
    case x :: y :: t 
        if y.isUpper => x.toLower :: '_' :: camelListToSnakeList(y :: t)
    case h :: t => h.toLower :: camelListToSnakeList(t)
    case Nil => Nil

private[sqala] def camelToSnake(s: String): String = camelListToSnakeList(s.toList).mkString

extension [A, B](a: A)
    private[sqala] def |>(f: A => B): B = f(a)

def queryToString(
    query: SqlQuery, 
    dialect: Dialect, 
    enableJdbcPrepare: Boolean
): (String, Array[Any]) =
    val printer = dialect.printer(enableJdbcPrepare)
    printer.printQuery(query)
    printer.sql -> printer.args.toArray

def statementToString(
    statement: SqlStatement, 
    dialect: Dialect, 
    enableJdbcPrepare: Boolean
): (String, Array[Any]) =
    val printer = dialect.printer(enableJdbcPrepare)
    printer.printStatement(statement)
    printer.sql -> printer.args.toArray
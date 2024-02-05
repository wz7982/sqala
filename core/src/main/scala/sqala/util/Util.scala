package sqala.util

import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.jdbc.Dialect

def camelListToSnakeList(s: List[Char]): List[Char] = s match
    case x :: y :: t if y.isUpper => x.toLower :: '_' :: camelListToSnakeList(y :: t)
    case h :: t => h.toLower :: camelListToSnakeList(t)
    case Nil => Nil

def camelToSnake(s: String): String = camelListToSnakeList(s.toList).mkString

def queryToString(query: SqlQuery, dialect: Dialect, prepare: Boolean): (String, Array[Any]) =
    val printer = dialect.printer(prepare)
    printer.printQuery(query)
    printer.sql -> printer.args.toArray

def statementToString(statement: SqlStatement, dialect: Dialect, prepare: Boolean): (String, Array[Any]) =
    val printer = dialect.printer(prepare)
    printer.printStatement(statement)
    printer.sql -> printer.args.toArray
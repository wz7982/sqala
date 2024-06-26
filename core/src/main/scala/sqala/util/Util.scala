package sqala.util

import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.printer.SqlPrinter

import scala.compiletime.{constValue, erasedValue}

def camelListToSnakeList(s: List[Char]): List[Char] = s match
    case x :: y :: t if y.isUpper => x.toLower :: '_' :: camelListToSnakeList(y :: t)
    case h :: t => h.toLower :: camelListToSnakeList(t)
    case Nil => Nil

def camelToSnake(s: String): String = camelListToSnakeList(s.toList).mkString

def queryToString(query: SqlQuery, printer: SqlPrinter): (String, Array[Any]) =
    printer.printQuery(query)
    printer.sql -> printer.args.toArray

def statementToString(statement: SqlStatement, printer: SqlPrinter): (String, Array[Any]) =
    printer.printStatement(statement)
    printer.sql -> printer.args.toArray

inline def fetchNames[T <: Tuple]: List[String] =
    inline erasedValue[T] match
        case _: EmptyTuple => Nil
        case _: (x *: xs) => constValue[x].asInstanceOf[String] :: fetchNames[xs]
package sqala.util

import sqala.ast.statement.*
import sqala.jdbc.Dialect

import scala.collection.mutable.ListBuffer

def camelListToSnakeList(s: List[Char]): List[Char] = s match
    case x :: y :: t if y.isUpper => x.toLower :: '_' :: camelListToSnakeList(y :: t)
    case h :: t => h.toLower :: camelListToSnakeList(t)
    case Nil => Nil

def camelToSnake(s: String): String = camelListToSnakeList(s.toList).mkString

def queryToString(query: SqlQuery, dialect: Dialect, prepare: Boolean): (String, Array[Any]) =
    dialect.printer.printQuery(query)
    dialect.printer.sql -> dialect.printer.args.toArray

def statementToString(statement: SqlStatement, dialect: Dialect, prepare: Boolean): (String, Array[Any]) =
    dialect.printer.printStatement(statement)
    dialect.printer.sql -> dialect.printer.args.toArray

def stringToList(s: String): List[String] =
    if s.isEmpty then Nil else     
        val chars = s.toArray
        var temp = ""
        val result = ListBuffer[String]()
        for i <- 0 until chars.size do
            val current = chars(i)
            if current.isUpper then 
                result.addOne(temp)
                temp = current.toLower.toString
            else temp = temp + current
        result.addOne(temp)
        result.toList
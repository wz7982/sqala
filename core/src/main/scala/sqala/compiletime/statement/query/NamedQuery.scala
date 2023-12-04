package sqala.compiletime.statement.query

import sqala.ast.expr.{SqlExpr, SqlBinaryOperator}
import sqala.ast.order.*
import sqala.ast.statement.*
import sqala.compiletime.*
import sqala.util.*

import scala.collection.mutable.ListBuffer
import scala.deriving.Mirror
import scala.language.dynamics

class NamedQuery[P <: Product : Entity](val table: Table[P, ?]) extends Dynamic:
    inline def applyDynamic(
        name: String
    )(using m: Mirror.ProductOf[P])(
        args: NamedQueryArgsType[m.MirroredElemTypes, m.MirroredElemLabels, StringToNamedQueryTokens[name.type]]
    ): Query[Tuple1[P], EmptyTuple] =
        val tokens = stringToList(name)

        val tableInfo = summon[Entity[P]].tableMeta
        val columnsMap = tableInfo.fieldNames.zip(tableInfo.columnNames).toMap

        val sqlExprArgs = AsSqlExpr.summonInstances[args.type].zip(args.toArray).map((instance, arg) => instance.asInstanceOf[AsSqlExpr[Any]].asSqlExpr(arg))

        val selectList = table.__cols.map(c => SqlSelectItem(c.toSqlExpr, None))
        val where = createWhere(whereTokens(tokens), sqlExprArgs, columnsMap, tableInfo.tableName)
        val orderBy = createOrderBy(orderTokens(tokens), columnsMap, tableInfo.tableName)
        val queryAst = SqlQuery.Select(select = selectList, from = table.toSqlTable :: Nil, where = where, orderBy = orderBy)

        new Query:
            override def ast: SqlQuery = queryAst

            override def cols: List[Column[?, ?, ?]] = Nil

    private[sqala] def whereTokens(tokens: List[String], s: String = ""): List[String] = tokens match
        case "find" :: "by" :: t => "by" :: whereTokens(t)
        case "not" :: "in" :: t => s :: "notIn" :: whereTokens(t)
        case "not" :: "like" :: t => s :: "notLike" :: whereTokens(t)
        case "not" :: "between" :: t => s :: "notBetween" :: whereTokens(t)
        case "starting" :: "with" :: t => s :: "startingWith" :: whereTokens(t)
        case "ending" :: "with" :: t => s :: "endingWith" :: whereTokens(t)
        case "is" :: "null" :: t => s :: "isNull" :: whereTokens(t)
        case "is" :: "not" :: "null" :: t => s :: "isNotNull" :: whereTokens(t)
        case "not" :: "null" :: t => s :: "isNotNull" :: whereTokens(t)
        case "greater" :: "than" :: "equal" :: t => s :: "greaterThanEqual" :: whereTokens(t)
        case "less" :: "than" :: "equal" :: t => s :: "lessThanEqual" :: whereTokens(t)
        case "greater" :: "than" :: t => s :: "greaterThan" :: whereTokens(t)
        case "less" :: "than" :: t => s :: "lessThan" :: whereTokens(t)
        case "order" :: "by" :: t => s :: Nil
        case h :: t => h match
            case "and" | "or" | "not" | "in" | "between" => s :: h :: whereTokens(t)
            case "is" | "equals" => s :: "equals" :: whereTokens(t)
            case "like" | "containing" => s :: "like" :: whereTokens(t)
            case _ => s match
                case "" => whereTokens(t, h)
                case _ => whereTokens(t, s + h.charAt(0).toUpper + h.substring(1, h.size))
        case Nil => s :: Nil

    private[sqala] def createWhere(tokens: List[String], args: List[SqlExpr], columnsMap: Map[String, String], tableName: String): Option[SqlExpr] =
        var where: Option[SqlExpr] = None
        val tokenIterator = tokens.iterator.buffered
        val argIterator = args.iterator
        while tokenIterator.hasNext do
            val token = tokenIterator.next
            token match
                case "by" | "and" | "or" =>
                    val columnName = columnsMap(tokenIterator.next)
                    val column = SqlExpr.Column(Some(tableName), columnName)
                    val predicate = tokenIterator.headOption
                    val tempCondition = predicate match
                        case Some("in") => SqlExpr.In(column, argIterator.next, false)
                        case Some("notIn") => SqlExpr.In(column, argIterator.next, false)
                        case Some("like") =>   
                            SqlExpr.Binary(column, SqlBinaryOperator.Like, SqlExpr.StringLiteral("%" + argIterator.next.asInstanceOf[SqlExpr.StringLiteral].string + "%"))
                        case Some("notLike") =>
                            SqlExpr.Binary(column, SqlBinaryOperator.NotLike, SqlExpr.StringLiteral("%" + argIterator.next.asInstanceOf[SqlExpr.StringLiteral].string + "%"))
                        case Some("startingWith") =>
                            SqlExpr.Binary(column, SqlBinaryOperator.Like, SqlExpr.StringLiteral(argIterator.next.asInstanceOf[SqlExpr.StringLiteral].string + "%"))
                        case Some("endingWith") =>
                            SqlExpr.Binary(column, SqlBinaryOperator.Like, SqlExpr.StringLiteral("%" + argIterator.next.asInstanceOf[SqlExpr.StringLiteral].string))
                        case Some("greaterThan") => SqlExpr.Binary(column, SqlBinaryOperator.GreaterThan, argIterator.next)
                        case Some("greaterThanEqual") => SqlExpr.Binary(column, SqlBinaryOperator.GreaterThanEqual, argIterator.next)
                        case Some("lessThan") => SqlExpr.Binary(column, SqlBinaryOperator.LessThan, argIterator.next)
                        case Some("lessThanEqual") => SqlExpr.Binary(column, SqlBinaryOperator.LessThanEqual, argIterator.next)
                        case Some("not") => SqlExpr.Binary(column, SqlBinaryOperator.NotEqual, argIterator.next)
                        case Some("between") => SqlExpr.Between(column, argIterator.next, argIterator.next, false)
                        case Some("notBetween") => SqlExpr.Between(column, argIterator.next, argIterator.next, true)
                        case Some("isNull") => SqlExpr.Binary(column, SqlBinaryOperator.Is, SqlExpr.Null)
                        case Some("isNotNull") => SqlExpr.Binary(column, SqlBinaryOperator.Is, SqlExpr.Null)
                        case _ => SqlExpr.Binary(column, SqlBinaryOperator.Equal, argIterator.next)
                    
                    (token, where) match
                        case (_, None) => where = Some(tempCondition)
                        case ("or", Some(w)) => where = Some(SqlExpr.Binary(w, SqlBinaryOperator.Or, tempCondition))
                        case (_, Some(w)) => where = Some(SqlExpr.Binary(w, SqlBinaryOperator.And, tempCondition))
                case _ =>
        where

    private[sqala] def orderTokens(tokens: List[String]): List[String] =
        val tokenIterator = tokens.iterator
        val result = ListBuffer[String]()
        var tempString = ""
        var start = false
        while tokenIterator.hasNext && !start do
            if tokenIterator.next == "order" && tokenIterator.next == "by" then start = true
        if start then
            val bufferedIterator = tokenIterator.buffered
            while tokenIterator.buffered.hasNext do
                (tempString, bufferedIterator.headOption) match
                    case (s, Some("asc")) => 
                        result.addOne(s)
                        result.addOne("asc")
                        tempString = ""
                        bufferedIterator.next
                    case (s, Some("desc")) =>
                        result.addOne(s)
                        result.addOne("desc")
                        tempString = ""
                        bufferedIterator.next
                    case ("", Some(_)) =>
                        tempString = bufferedIterator.next
                    case (s, Some(_)) =>
                        val next = bufferedIterator.next
                        tempString = s + next.charAt(0).toUpper + next.substring(1, next.size)
                    case (s, None) =>
                        result.addOne(tempString)
        result.toList


    private[sqala] def createOrderBy(tokens: List[String], columnsMap: Map[String, String], tableName: String): List[SqlOrderBy] =
        if tokens.isEmpty then Nil else
            val result = ListBuffer[SqlOrderBy]()
            val pair = tokens.grouped(2)
            while pair.hasNext do
                val next = pair.next
                next(1) match
                    case "desc" => result.addOne(SqlOrderBy(SqlExpr.Column(Some(tableName), columnsMap(next(0))), Some(SqlOrderByOption.Desc)))
                    case _ => result.addOne(SqlOrderBy(SqlExpr.Column(Some(tableName), columnsMap(next(0))), Some(SqlOrderByOption.Asc)))
            result.toList

package sqala.parser

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.{SqlOrderItem, SqlOrderOption}
import sqala.ast.param.SqlParam
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlUnionType}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTableAlias, SqlTable}

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

object SqlParser extends StandardTokenParsers:
    class SqlLexical extends StdLexical:
        override protected def processIdent(name: String): Token =
            val upperCased = name.toUpperCase.nn
            if reserved.contains(upperCased) then Keyword(upperCased) else Identifier(name)

        override def token: Parser[Token] =
            identChar ~ rep(identChar | digit) ^^ {
                case first ~ rest => processIdent((first :: rest).mkString(""))
            } |
            '\"' ~> (identChar ~ rep(identChar | digit)) <~ '\"' ^^ {
                   case first ~ rest => Identifier((first :: rest).mkString(""))
            } |
            '`' ~> (identChar ~ rep(identChar | digit)) <~ '`' ^^ {
                case first ~ rest => Identifier((first :: rest).mkString(""))
            } |
            rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
                case i ~ None => NumericLit(i.mkString(""))
                case i ~ Some(d) => NumericLit(i.mkString("") + "." + d.mkString(""))
            } |
            '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ {
                case _ ~ chars ~ _ => StringLit(chars.mkString(""))
            } |
            EofCh ^^^ EOF |
            delim |
            failure("illegal character")

    override val lexical: SqlLexical = new SqlLexical

    lexical.reserved.addAll(
       List(
           "CAST", "AS", "AND", "XOR", "OR", "OVER", "BY", "PARTITION", "ORDER", "DISTINCT", "NOT",
           "CASE", "WHEN", "THEN", "ELSE", "END", "ASC", "DESC", "TRUE", "FALSE", "NULL",
           "BETWEEN", "IN", "LIKE", "IS",
           "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "LIMIT", "OFFSET",
           "JOIN", "OUTER", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ON", "LATERAL",
           "UNION", "EXCEPT", "INTERSECT", "ALL", "ANY", "EXISTS", "SOME", "COUNT",
           "INTERVAL", "EXTRACT", "YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"
       )
    )

    lexical.delimiters.addAll(
       List(
           "+", "-", "*", "/", "%", "=", "<>", "!=", ">", ">=", "<", "<=", "(", ")", ",", ".", "`", "\"", "?", "::"
       )
    )

    def expr: Parser[SqlExpr] = or

    def or: Parser[SqlExpr] = and * ("OR" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlExpr.Binary(l, SqlBinaryOperator.Or, r) })

    def and: Parser[SqlExpr] = relation * ("AND" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlExpr.Binary(l, SqlBinaryOperator.And, r) })

    def relation: Parser[SqlExpr] =
        add ~ rep(
            ("=" | "<>" | "!=" | ">" | ">=" | "<" | "<=") ~ add ^^ {
                case op ~ right => (op, right)
            } |
            "IS" ~ opt("NOT") ~ "NULL" ^^ {
                case _ ~ n ~ right => (n.isDefined, "is", right)
            } |
            opt("NOT") ~ "BETWEEN" ~ add ~ "AND" ~ add ^^ {
                case n ~ _ ~ start ~ _ ~ end => (n.isDefined, "between", start, end)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ select ~ ")" ^^ {
                case n ~ _ ~ _ ~ in ~ _ => (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ {
                case n ~ _ ~ _ ~ in ~ _ => (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "LIKE" ~ add ^^ {
                case n ~ _ ~ right => (n.isDefined, "like", right)
            }
        ) ^^ {
            case left ~ elems => elems.foldLeft(left) {
                case (acc, ("=", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.Equal, right)
                case (acc, ("<>", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.NotEqual, right)
                case (acc, ("!=", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.NotEqual, right)
                case (acc, (">", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.GreaterThan, right)
                case (acc, (">=", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.GreaterThanEqual, right)
                case (acc, ("<", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.LessThan, right)
                case (acc, ("<=", right)) => SqlExpr.Binary(acc, SqlBinaryOperator.LessThanEqual, right)
                case (acc, (false, "is", _)) => SqlExpr.NullTest(acc, false)
                case (acc, (true, "is", _)) => SqlExpr.NullTest(acc, true)
                case (acc, (not: Boolean, "between", l: SqlExpr, r: SqlExpr)) => SqlExpr.Between(acc, l, r, not)
                case (acc, (false, "in", in: SqlExpr.SubQuery)) => SqlExpr.Binary(acc, SqlBinaryOperator.In, in)
                case (acc, (true, "in", in: SqlExpr.SubQuery)) => SqlExpr.Binary(acc, SqlBinaryOperator.NotIn, in)
                case (acc, (false, "in", in: List[?])) => SqlExpr.Binary(acc, SqlBinaryOperator.In, SqlExpr.Tuple(in.asInstanceOf[List[SqlExpr]]))
                case (acc, (true, "in", in: List[?])) => SqlExpr.Binary(acc, SqlBinaryOperator.NotIn, SqlExpr.Tuple(in.asInstanceOf[List[SqlExpr]]))
                case (acc, (false, "like", expr: SqlExpr)) => SqlExpr.Binary(acc, SqlBinaryOperator.Like, expr)
                case (acc, (true, "like", expr: SqlExpr)) => SqlExpr.Binary(acc, SqlBinaryOperator.NotLike, expr)
                case _ => SqlExpr.Null
            }
        }

    def add: Parser[SqlExpr] =
        mul * (
            "+" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Plus, b)) |
            "-" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Minus, b))
        )

    def mul: Parser[SqlExpr] =
        primary * (
            "*" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Times, b)) |
            "/" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Div, b)) |
            "%" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Mod, b))
        )

    def column: Parser[SqlExpr] =
        ident ~ opt("." ~> ident) ^^ {
            case id ~ None => SqlExpr.Column(None, id)
            case table ~ Some(column) => SqlExpr.Column(Some(table), column)
        }

    def primary: Parser[SqlExpr] =
        literal |
        caseWhen |
        cast |
        windowFunction |
        function |
        "(" ~> union <~ ")" |
        column |
        subLink |
        interval |
        extract |
        "(" ~> expr <~ ")"

    def function: Parser[SqlExpr] =
        "COUNT" ~ "(" ~ "*" ~ ")" ^^ (_ => SqlExpr.Func("COUNT", Nil, None, Nil)) |
        (ident | "COUNT") ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlExpr.Func(funcName.toUpperCase.nn, args, None, Nil)
        } |
        (ident | "COUNT") ~ ("(" ~ "DISTINCT" ~> expr <~ ")") ^^ {
            case funcName ~ arg => SqlExpr.Func(funcName.toUpperCase.nn, arg :: Nil, Some(SqlParam.Distinct), Nil)
        }

    def over: Parser[(List[SqlExpr], List[SqlOrderItem])] =
        "(" ~> "PARTITION" ~> "BY" ~> rep1sep(expr, ",") ~ opt("ORDER" ~> "BY" ~> rep1sep(order, ",")) <~ ")" ^^ {
            case partition ~ order => (partition, order.getOrElse(Nil))
        } |
        "(" ~> "ORDER" ~> "BY" ~> rep1sep(order, ",") <~ ")" ^^ { o =>
            (Nil, o)
        }

    def windowFunction: Parser[SqlExpr] =
        function ~ "OVER" ~ over ^^ {
            case agg ~ _ ~ o => SqlExpr.Window(agg, o._1, o._2, None)
        }

    def subLink: Parser[SqlExpr] =
        "ANY" ~> "(" ~> union <~ ")" ^^ { u =>
            SqlExpr.SubLink(u.query, SqlSubLinkType.Any)
        } |
        "SOME" ~> "(" ~> union <~ ")" ^^ { u =>
            SqlExpr.SubLink(u.query, SqlSubLinkType.Some)
        } |
        "ALL" ~> "(" ~> union <~ ")" ^^ { u =>
            SqlExpr.SubLink(u.query, SqlSubLinkType.All)
        } |
        "EXISTS" ~> "(" ~> union <~ ")" ^^ { u =>
            SqlExpr.SubLink(u.query, SqlSubLinkType.Exists)
        }

    def order: Parser[SqlOrderItem] =
        expr ~ opt("ASC" | "DESC") ^^ {
            case e ~ Some("DESC") => SqlOrderItem(e, Some(SqlOrderOption.Desc), None)
            case e ~ _ => SqlOrderItem(e, Some(SqlOrderOption.Asc), None)
        }

    def cast: Parser[SqlExpr] =
        "CAST" ~> ("(" ~> expr ~ "AS" ~ ident <~ ")") ^^ {
            case expr ~ _ ~ castType => SqlExpr.Cast(expr, SqlCastType.Custom(castType.toUpperCase.nn))
        }

    def caseWhen: Parser[SqlExpr] =
        "CASE" ~>
            rep1("WHEN" ~> expr ~ "THEN" ~ expr ^^ { case e ~ _ ~ te => SqlCase(e, te) }) ~
            opt("ELSE" ~> expr) <~ "END" ^^ {
                case branches ~ default => SqlExpr.Case(branches, default.getOrElse(SqlExpr.Null))
            }

    def interval: Parser[SqlExpr] =
        "INTERVAL" ~> numericLit ~ ("YEAR" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND") ^^ {
            case n ~ u =>
                val timeUnit = u match
                    case "YEAR" => SqlTimeUnit.Year
                    case "MONTH" => SqlTimeUnit.Month
                    case "WEEK" => SqlTimeUnit.Week
                    case "DAY" => SqlTimeUnit.Day
                    case "HOUR" => SqlTimeUnit.Hour
                    case "MINUTE" => SqlTimeUnit.Minute
                    case "SECOND" => SqlTimeUnit.Second
                SqlExpr.Interval(n.toDouble, timeUnit)
        }

    def extract: Parser[SqlExpr] =
        "EXTRACT" ~> "(" ~> ("YEAR" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND") ~ "FROM" ~ expr <~ ")" ^^ {
            case u ~ _ ~ e =>
                val timeUnit = u match
                    case "YEAR" => SqlTimeUnit.Year
                    case "MONTH" => SqlTimeUnit.Month
                    case "WEEK" => SqlTimeUnit.Week
                    case "DAY" => SqlTimeUnit.Day
                    case "HOUR" => SqlTimeUnit.Hour
                    case "MINUTE" => SqlTimeUnit.Minute
                    case "SECOND" => SqlTimeUnit.Second
                SqlExpr.Extract(timeUnit, e)
        }

    def literal: Parser[SqlExpr] =
        numericLit ^^ (i => SqlExpr.NumberLiteral(BigDecimal(i))) |
        "-" ~> numericLit ^^ (i => SqlExpr.NumberLiteral(BigDecimal(i) * -1)) |
        stringLit ^^ (xs => SqlExpr.StringLiteral(xs)) |
        "TRUE" ^^ (_ => SqlExpr.BooleanLiteral(true)) |
        "FALSE" ^^ (_ => SqlExpr.BooleanLiteral(false)) |
        "NULL" ^^ (_ => SqlExpr.Null)

    def unionType: Parser[SqlUnionType] =
        "UNION" ~> opt("ALL") ^^ {
            case None => SqlUnionType.Union
            case _ => SqlUnionType.UnionAll
        } |
        "EXCEPT" ~> opt("ALL") ^^ {
            case None => SqlUnionType.Except
            case _ => SqlUnionType.ExceptAll
        } |
        "INTERSECT" ~> opt("ALL") ^^ {
            case None => SqlUnionType.Intersect
            case _ => SqlUnionType.IntersectAll
        }

    def query: Parser[SqlExpr.SubQuery] =
        select |
        "(" ~> union <~ ")"

    def union: Parser[SqlExpr.SubQuery] =
        query ~ rep(
            unionType ~ query ^^ {
                case t ~ s => (t, s)
            }
        ) ^^ {
            case s ~ unions => unions.foldLeft(s):
                case (l, r) => SqlExpr.SubQuery(SqlQuery.Union(l.query, r._1, r._2.query))
        }

    def select: Parser[SqlExpr.SubQuery] =
        "SELECT" ~> opt("DISTINCT") ~ selectItems ~ opt(from) ~ opt(where) ~ opt(groupBy) ~ opt(orderBy) ~ opt(limit) ^^ {
            case distinct ~ s ~ f ~ w ~ g ~ o ~ l =>
                val param = if distinct.isDefined then Some(SqlParam.Distinct) else None
                SqlExpr.SubQuery(
                    SqlQuery.Select(param, s, f.getOrElse(Nil), w, g.map(_._1.map(i => SqlGroupItem.Singleton(i))).getOrElse(Nil), g.flatMap(_._2), o.getOrElse(Nil), l)
                )
        }

    def selectItems: Parser[List[SqlSelectItem]] =
        rep1sep(selectItem, ",")

    def selectItem: Parser[SqlSelectItem] =
        "*" ^^ (_ => SqlSelectItem.Wildcard(None)) |
        ident ~ "." ~ "*" ^^ {
            case e ~ _ ~ _ => SqlSelectItem.Wildcard(Some(e))
        } |
        expr ~ opt(opt("AS") ~> ident) ^^ {
            case expr ~ alias => SqlSelectItem.Item(expr, alias)
        }

    def simpleTable: Parser[SqlTable] =
        ident ~ opt(opt("AS") ~> ident ~ opt("(" ~> rep1sep(ident, ",") <~ ")")) ^^ {
            case table ~ alias =>
                val tableAlias = alias.map:
                    case ta ~ ca => SqlTableAlias(ta, ca.getOrElse(Nil))
                SqlTable.Range(table, tableAlias)
        } |
        opt("LATERAL") ~ ("(" ~> union <~ ")") ~ (opt("AS") ~> ident ~ opt("(" ~> rep1sep(ident, ",") <~ ")")) ^^ {
            case lateral ~ s ~ (tableAlias ~ columnAlias) =>
                SqlTable.SubQuery(s.query, lateral.isDefined, Some(SqlTableAlias(tableAlias, columnAlias.getOrElse(Nil))))
        }

    def joinType: Parser[SqlJoinType] =
        "LEFT" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.Left) |
        "RIGHT" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.Right) |
        "FULL" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.Full) |
        "CROSS" ~ "JOIN" ^^ (_ => SqlJoinType.Cross) |
        "INNER" ~ "JOIN" ^^ (_ => SqlJoinType.Inner) |
        "JOIN" ^^ (_ => SqlJoinType.Inner)

    def table: Parser[SqlTable] =
        simpleTable |
        "(" ~> joinTable <~ ")"

    def joinTable: Parser[SqlTable] =
        table ~ rep(
            joinType ~ table ~ opt("ON" ~> expr) ^^ {
                case jt ~ t ~ o => (jt, t, o)
            }
        ) ^^ {
            case t ~ joins => joins.foldLeft(t) {
                case (l, r) => SqlTable.Join(l, r._1, r._2, r._3.map(SqlJoinCondition.On(_)), None)
            }
        }

    def from: Parser[List[SqlTable]] =
        "FROM" ~> rep1sep(joinTable, ",")

    def where: Parser[SqlExpr] =
        "WHERE" ~> expr

    def groupBy: Parser[(List[SqlExpr], Option[SqlExpr])] =
        "GROUP" ~> "BY" ~> rep1sep(expr, ",") ~ opt("HAVING" ~> expr) ^^ {
            case g ~ h => (g, h)
        }

    def orderBy: Parser[List[SqlOrderItem]] =
        "ORDER" ~> "BY" ~> rep1sep(order, ",")

    def limit: Parser[SqlLimit] =
        "LIMIT" ~ numericLit ~ opt("OFFSET" ~> numericLit) ^^ {
            case _ ~ limit ~ offset =>
                val limitInt = BigDecimal(limit).setScale(0, BigDecimal.RoundingMode.HALF_UP).toInt
                val offsetInt = offset
                    .map(BigDecimal(_).setScale(0, BigDecimal.RoundingMode.HALF_UP).toInt)
                    .getOrElse(0)
                SqlLimit(
                    SqlExpr.NumberLiteral(limitInt),
                    SqlExpr.NumberLiteral(offsetInt)
                )
        }

    def parseExpr(text: String): SqlExpr =
        phrase(expr)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw ParseException(e.toString)

    def parseColumn(text: String): SqlExpr =
        phrase(column)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw ParseException(e.toString)

    def parseIdent(text: String): String =
        phrase(ident)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw ParseException(e.toString)

    def parseQuery(text: String): SqlQuery =
        phrase(union)(new lexical.Scanner(text)) match
            case Success(result, _) => result.query
            case e => throw ParseException(e.toString)

class ParseException(msg: String) extends Exception:
    override def toString: String =
        s"sqala.parser.ParseException: \n$msg"
package sqala.dynamic.parser

import sqala.ast.expr.*
import sqala.ast.group.{SqlGroupBy, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering, SqlOrderingItem}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSetOperator}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

class SqlParser extends StandardTokenParsers:
    class SqlLexical extends StdLexical:
        override protected def processIdent(name: String): Token =
            val upperCased = name.toUpperCase
            if reserved.contains(upperCased) then 
                Keyword(upperCased) 
            else Identifier(name)

        def numeric: Parser[Token] =
            rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
                case i ~ None => NumericLit(i.mkString)
                case i ~ Some(d) => NumericLit(i.mkString + "." + d.mkString)
            } |
            '.' ~ rep1(digit) ^^ {
                case _ ~ d => NumericLit("." + d.mkString)
            }

        def string: Parser[String] =
            escapeSequence | 
            normalChar

        def escapeSequence: Parser[String] =
            '\\' ~> escapeChar ^^ {
                case 'n' => "\\n"
                case 't' => "\\t"
                case 'r' => "\\r"
                case 'b' => "\\b"
                case 'f' => "\\f"
                case '\\' => "\\\\"
                case '\'' => "\\'"
                case c => s"\\$c"
            } |
            elem('\'') ~ elem('\'') ^^ {
                case _ ~ _ => "''"
            }

        def escapeChar: Parser[Char] =
            elem("escape character", c => "ntrbf\\'".contains(c)) |
            elem("character", _ => true)

        def normalChar: Parser[String] =
            elem(
                "character", 
                c => 
                    c != '\'' && 
                    c != '\\' && 
                    c != '\n' && 
                    c != '\t' && 
                    c != '\r' && 
                    c != '\b' &&
                    c != '\f' &&
                    c != '\\'
            ) ^^ (c => c.toString)

        override def token: Parser[Token] =
            identChar ~ rep(identChar | digit) ^^ {
                case first ~ rest => processIdent((first :: rest).mkString)
            } |
            '\"' ~> identChar ~ rep(identChar | digit) <~ '\"' ^^ {
                case first ~ rest => Identifier((first :: rest).mkString)
            } |
            '`' ~> (identChar ~ rep(identChar | digit)) <~ '`' ^^ {
                case first ~ rest => Identifier((first :: rest).mkString)
            } |
            '[' ~> (identChar ~ rep(identChar | digit)) <~ ']' ^^ {
                case first ~ rest => Identifier((first :: rest).mkString)
            } |
            numeric ~ opt((elem('e') | elem('E')) ~ opt(elem('+') | elem('-')) ~ rep1(digit)) ^^ {
                case n ~ Some(_ ~ s ~ v) =>
                    NumericLit(n.chars + "E" + s.getOrElse("") + v.mkString)
                case n ~ None =>
                    n
            } |
            '\'' ~ rep(string) ~ '\'' ^^ {
                case _ ~ s ~ _ => StringLit(s.mkString)
            } |
            EofCh ^^^ EOF |
            delim |
            failure("illegal character")

    override val lexical: SqlLexical = new SqlLexical

    lexical.reserved.addAll(
        List(
            "CAST", "AS", "AND", "OR", "OVER", "BY", "PARTITION", "ORDER", "DISTINCT", "NOT",
            "CASE", "WHEN", "THEN", "ELSE", "END", 
            "TRUE", "FALSE", "NULL",
            "BETWEEN", "IN", "LIKE", "IS",
            "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "LIMIT", "OFFSET",
            "JOIN", "OUTER", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "ON", "LATERAL",
            "UNION", "EXCEPT", "INTERSECT", "ALL", "ANY", "EXISTS", "SOME",
            "INTERVAL", "EXTRACT", "YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND",
            "TIMESTAMP", "DATE", "TIME",
            "FILTER", "WITHIN", 
            "ASC", "DESC", "NULLS", "FIRST", "LAST", "PERCENT", "WITH", "TIES",
            "ARRAY", "ONLY", "NEXT", "FETCH",
            "CURRENT", "ROW", "UNBOUNDED", "PRECEDING", "FOLLOWING",
            "ROWS", "RANGE", "GROUPS",
            "CUBE", "ROLLUP", "GROUPING", "SETS",
            "VALUES"
        )
    )

    lexical.delimiters.addAll(
        List(
            "+", "-", "*", "/", "%", "||", "->", "->>",
            "=", "<>", "!=", ">", ">=", "<", "<=", 
            "(", ")", ",", ".", "`", "\"", "[", "]", "::"
        )
    )

    def expr: Parser[SqlExpr] = 
        or

    def or: Parser[SqlExpr] = 
        and * ("OR" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlExpr.Binary(l, SqlBinaryOperator.Or, r) })

    def and: Parser[SqlExpr] = 
        not * ("AND" ^^^ { (l: SqlExpr, r: SqlExpr) => SqlExpr.Binary(l, SqlBinaryOperator.And, r) })

    def not: Parser[SqlExpr] = 
        "NOT" ~> relation ^^ (SqlExpr.Unary(SqlUnaryOperator.Not, _)) |
        relation

    def relation: Parser[SqlExpr] =
        concat ~ rep(
            ("=" | "<>" | "!=" | ">" | ">=" | "<" | "<=") ~ concat ^^ {
                case op ~ right => 
                    (op, right)
            } |
            "IS" ~ opt("NOT") ~ "NULL" ^^ {
                case _ ~ n ~ right => 
                    (n.isDefined, "is", right)
            } |
            opt("NOT") ~ "BETWEEN" ~ concat ~ "AND" ~ concat ^^ {
                case n ~ _ ~ start ~ _ ~ end => 
                    (n.isDefined, "between", start, end)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ select ~ ")" ^^ {
                case n ~ _ ~ _ ~ in ~ _ => 
                    (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ rep1sep(concat, ",") ~ ")" ^^ {
                case n ~ _ ~ _ ~ in ~ _ => 
                    (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "LIKE" ~ concat ^^ {
                case n ~ _ ~ right => 
                    (n.isDefined, "like", right)
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
                case _ => SqlExpr.NullLiteral
            }
        }

    def concat: Parser[SqlExpr] =
        add * (
            "||" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Concat, b))
        )

    def add: Parser[SqlExpr] =
        mul * (
            "+" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Plus, b)) |
            "-" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Minus, b))
        )

    def mul: Parser[SqlExpr] =
        sign * (
            "*" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Times, b)) |
            "/" ^^^ ((a, b) => SqlExpr.Binary(a, SqlBinaryOperator.Div, b))
        )
    
    def sign: Parser[SqlExpr] = 
        "+" ~> convert ^^ (SqlExpr.Unary(SqlUnaryOperator.Positive, _)) |
        "-" ~> convert ^^ (SqlExpr.Unary(SqlUnaryOperator.Negative, _)) |
        convert

    def convert: Parser[SqlExpr] =
        primary ~ rep("::" ~> ident) ^^ {
            case expr ~ types =>
                types.foldLeft(expr) {
                    case (acc, castType) =>
                        SqlExpr.Cast(acc, SqlType.Custom(castType.toUpperCase))
                }
        }

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
        "(" ~> query <~ ")" ^^ (q => SqlExpr.SubQuery(q)) |
        column |
        subLink |
        interval |
        extract |
        "(" ~> expr <~ ")" |
        tuple |
        array

    def tuple: Parser[SqlExpr.Tuple] =
        "(" ~> repsep(expr, ",") <~ ")" ^^ (SqlExpr.Tuple(_))

    def array: Parser[SqlExpr] =
        "ARRAY" ~ "[" ~> repsep(expr, ",") <~ "]" ^^ (SqlExpr.Array(_))

    def function: Parser[SqlExpr] =
        "COUNT" ~ "(" ~ "*" ~ ")" ~> opt("FILTER" ~ "(" ~> where <~ ")") ^^ {
            case f => 
                SqlExpr.StandardFunc(None, "COUNT", Nil, Nil, Nil, f)
        } |
        ident ~ ("(" ~> opt(quantifier) ~ repsep(expr, ",") ~ opt(orderBy) <~ ")")
            ~ opt("WITHIN" ~ "GROUP" ~ "(" ~> orderBy <~ ")")
            ~ opt("FILTER" ~ "(" ~> where <~ ")") 
        ^^ {
            case ident ~ (quantifier ~ args ~ orderBy) ~ withinGroup ~ filter =>
                SqlExpr.StandardFunc(quantifier, ident.toUpperCase, args, orderBy.getOrElse(Nil), withinGroup.getOrElse(Nil), filter)
        }

    def frameBound: Parser[SqlWindowFrameBound] =
        "CURRENT" ~ "ROW" ^^ (_ => SqlWindowFrameBound.CurrentRow) |
        "UNBOUNDED" ~ "PRECEDING" ^^ (_ => SqlWindowFrameBound.UnboundedPreceding) |
        numericLit <~ "PRECEDING" ^^ (n => SqlWindowFrameBound.Preceding(BigDecimal(n).toInt)) |
        "UNBOUNDED" ~ "FOLLOWING" ^^ (_ => SqlWindowFrameBound.UnboundedPreceding) |
        numericLit <~ "PRECEDING" ^^ (n => SqlWindowFrameBound.Preceding(BigDecimal(n).toInt))

    def frame: Parser[SqlWindowFrame] =
        def unit(s: String): SqlWindowFrameUnit =
            s match
                case "ROWS" => SqlWindowFrameUnit.Rows
                case "RANGE" => SqlWindowFrameUnit.Range
                case "GROUPS" => SqlWindowFrameUnit.Groups
        ("ROWS" | "RANGE" | "GROUPS") ~ frameBound ^^ {
            case u ~ b =>
                SqlWindowFrame.Start(unit(u), b, None)
        } |
        ("ROWS" | "RANGE" | "GROUPS") ~ "BETWEEN" ~ frameBound ~ "AND" ~ frameBound ^^ {
            case u ~ _ ~ s ~ _ ~ e =>
                SqlWindowFrame.Between(unit(u), s, e, None)
        }

    def over: Parser[(List[SqlExpr], List[SqlOrderingItem], Option[SqlWindowFrame])] =
        "(" ~> 
            opt("PARTITION" ~> "BY" ~> rep1sep(expr, ",")) ~ opt(orderBy) ~ opt(frame)
        <~ ")" ^^ {
            case partition ~ order ~ frame => 
                (partition.getOrElse(Nil), order.getOrElse(Nil), frame)
        }

    def windowFunction: Parser[SqlExpr] =
        function ~ "OVER" ~ over ^^ {
            case agg ~ _ ~ o => 
                SqlExpr.Window(agg, SqlWindow(o._1, o._2, o._3))
        }

    def subLink: Parser[SqlExpr] =
        "ANY" ~> "(" ~> query <~ ")" ^^ { u =>
            SqlExpr.SubLink(SqlSubLinkQuantifier.Any, u)
        } |
        "SOME" ~> "(" ~> query <~ ")" ^^ { u =>
            SqlExpr.SubLink(SqlSubLinkQuantifier.Some, u)
        } |
        "ALL" ~> "(" ~> query <~ ")" ^^ { u =>
            SqlExpr.SubLink(SqlSubLinkQuantifier.All, u)
        } |
        "EXISTS" ~> "(" ~> query <~ ")" ^^ { u =>
            SqlExpr.SubLink(SqlSubLinkQuantifier.Exists, u)
        }

    def order: Parser[SqlOrderingItem] =
        def nullsOrdering(s: String): SqlNullsOrdering =
            s match
                case "FIRST" => SqlNullsOrdering.First
                case _ => SqlNullsOrdering.Last
        expr ~ opt("ASC" | "DESC") ~ opt("NULLS" ~> ("FIRST" | "LAST")) ^^ {
            case e ~ Some("DESC") ~ n =>
                SqlOrderingItem(e, Some(SqlOrdering.Desc), n.map(nullsOrdering))
            case e ~ _ ~ n => 
                SqlOrderingItem(e, Some(SqlOrdering.Asc), n.map(nullsOrdering))
        }

    def cast: Parser[SqlExpr] =
        "CAST" ~> ("(" ~> expr ~ "AS" ~ ident <~ ")") ^^ {
            case expr ~ _ ~ castType => 
                SqlExpr.Cast(expr, SqlType.Custom(castType.toUpperCase))
        }

    def caseWhen: Parser[SqlExpr] =
        "CASE" ~>
            opt(expr) ~
            rep1("WHEN" ~> expr ~ "THEN" ~ expr ^^ { case e ~ _ ~ te => SqlWhen(e, te) }) ~
            opt("ELSE" ~> expr) <~ "END" ^^ {
                case None ~ branches ~ default => 
                    SqlExpr.Case(branches, default.orElse(Some(SqlExpr.NullLiteral)))
                case Some(test) ~ branches ~ default => 
                    SqlExpr.SimpleCase(test, branches, default.orElse(Some(SqlExpr.NullLiteral)))
            }

    def interval: Parser[SqlExpr] =
        "INTERVAL" ~> numericLit ~ ("YEAR" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND") ^^ {
            case n ~ u =>
                val timeUnit = u match
                    case "YEAR" => SqlTimeUnit.Year
                    case "MONTH" => SqlTimeUnit.Month
                    case "DAY" => SqlTimeUnit.Day
                    case "HOUR" => SqlTimeUnit.Hour
                    case "MINUTE" => SqlTimeUnit.Minute
                    case "SECOND" => SqlTimeUnit.Second
                SqlExpr.IntervalLiteral(n, SqlIntervalField.Single(timeUnit))
        }

    def extract: Parser[SqlExpr] =
        "EXTRACT" ~> "(" ~> ("YEAR" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND") ~ "FROM" ~ expr <~ ")" ^^ {
            case u ~ _ ~ e =>
                val timeUnit = u match
                    case "YEAR" => SqlTimeUnit.Year
                    case "MONTH" => SqlTimeUnit.Month
                    case "DAY" => SqlTimeUnit.Day
                    case "HOUR" => SqlTimeUnit.Hour
                    case "MINUTE" => SqlTimeUnit.Minute
                    case "SECOND" => SqlTimeUnit.Second
                SqlExpr.ExtractFunc(timeUnit, e)
        }

    def literal: Parser[SqlExpr] =
        opt("+") ~> numericLit ^^ (i => SqlExpr.NumberLiteral(BigDecimal(i))) |
        "-" ~> numericLit ^^ (i => SqlExpr.NumberLiteral(BigDecimal(i) * -1)) |
        stringLit ^^ (xs => SqlExpr.StringLiteral(xs)) |
        "TRUE" ^^ (_ => SqlExpr.BooleanLiteral(true)) |
        "FALSE" ^^ (_ => SqlExpr.BooleanLiteral(false)) |
        "NULL" ^^ (_ => SqlExpr.NullLiteral) |
        timestampLiteral |
        dateLiteral |
        timeLiteral

    def timestampLiteral: Parser[SqlExpr] =
        val check: String => Boolean = s =>
            try
                LocalDateTime.parse(s)
                true
            catch
                case _: Exception =>
                    try
                        val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss")
                        LocalDateTime.parse(s, formatter)
                        true
                    catch
                        case _: Exception =>
                            try
                                val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm:ss")
                                LocalDateTime.parse(s, formatter)
                                true
                            catch
                                case _: Exception =>
                                    false
        "TIMESTAMP" ~> stringLit.filter(check) ^^ { d =>
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, d)
        }

    def dateLiteral: Parser[SqlExpr] =
        val check: String => Boolean = s =>
            try
                LocalDate.parse(s)
                true
            catch
                case _: Exception =>
                    try
                        val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
                        LocalDate.parse(s, formatter)
                        true
                    catch
                        case _: Exception =>
                            try
                                val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")
                                LocalDate.parse(s, formatter)
                                true
                            catch
                                case _: Exception =>
                                    false
        "DATE" ~> stringLit.filter(check) ^^ { d =>
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, d)
        }

    def timeLiteral: Parser[SqlExpr] =
        val check: String => Boolean = s =>
            try
                LocalTime.parse(s)
                true
            catch
                case _: Exception =>
                    false
        "TIME" ~> stringLit.filter(check) ^^ { d =>
            SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Time, d)
        }

    def setOperator: Parser[SqlSetOperator] =
        "UNION" ~> opt(quantifier) ^^ (SqlSetOperator.Union(_)) |
        "EXCEPT" ~> opt(quantifier) ^^ (SqlSetOperator.Except(_)) |
        "INTERSECT" ~> opt(quantifier) ^^ (SqlSetOperator.Intersect(_))

    def quantifier: Parser[SqlQuantifier] =
        ("ALL" | "DISTINCT") ^^ {
            case "ALL" => SqlQuantifier.All
            case _ => SqlQuantifier.Distinct
        }

    def query: Parser[SqlQuery] =
        set

    def set: Parser[SqlQuery] =
        simpleQuery ~ rep(
            setOperator ~ simpleQuery ^^ {
                case t ~ s => (t, s)
            }
        ) ^^ {
            case s ~ unions => unions.foldLeft(s) {
                case (l, r) => SqlQuery.Set(l, r._1, r._2, Nil, None, None)
            }
        }

    def simpleQuery: Parser[SqlQuery] =
        select |
        values |
        "(" ~> query <~ ")"

    def select: Parser[SqlQuery] =
        "SELECT" ~> 
            opt(quantifier) ~ 
            selectItems ~ 
            opt(from) ~ 
            opt(where) ~ 
            opt(groupBy) ~
            opt(having) ~
            opt(orderBy) ~ 
            opt(limit) 
        ^^ {
            case q ~ s ~ f ~ w ~ g ~ h ~ o ~ l =>
                SqlQuery.Select(
                    q, 
                    s, 
                    f.getOrElse(Nil), 
                    w, 
                    g,
                    h,
                    Nil, 
                    o.getOrElse(Nil), 
                    l.flatten,
                    None
                )
        }

    def values: Parser[SqlQuery] =
        "VALUES" ~> rep1sep(opt("ROW") ~> tuple, ",") ^^ {
            case v =>
                SqlQuery.Values(v.map(_.items), None)
        }

    def selectItems: Parser[List[SqlSelectItem]] =
        rep1sep(selectItem, ",")

    def selectItem: Parser[SqlSelectItem] =
        "*" ^^ (_ => SqlSelectItem.Asterisk(None)) |
        ident ~ "." ~ "*" ^^ {
            case e ~ _ ~ _ => SqlSelectItem.Asterisk(Some(e))
        } |
        expr ~ opt(opt("AS") ~> ident) ^^ {
            case expr ~ alias => SqlSelectItem.Expr(expr, alias)
        }

    def tableAlias: Parser[SqlTableAlias] =
        opt("AS") ~> ident ~ opt("(" ~> rep1sep(ident, ",") <~ ")") ^^ {
            case ta ~ ca => 
                SqlTableAlias(ta, ca.getOrElse(Nil))
        }

    def simpleTable: Parser[SqlTable] =
        ident ~ opt(tableAlias) ^^ {
            case table ~ alias =>
                SqlTable.Standard(table, None, alias, None, None)
        } |
        opt("LATERAL") ~ ("(" ~> set <~ ")") ~ opt(tableAlias) ^^ {
            case lateral ~ s ~ alias =>
                SqlTable.SubQuery(s, lateral.isDefined, alias, None)
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
                case (l, r) => 
                    SqlTable.Join(l, r._1, r._2, r._3.map(SqlJoinCondition.On(_)))
            }
        }

    def from: Parser[List[SqlTable]] =
        "FROM" ~> rep1sep(joinTable, ",")

    def where: Parser[SqlExpr] =
        "WHERE" ~> expr

    def groupingItem: Parser[SqlGroupingItem] =
        "CUBE" ~ "(" ~> rep1sep(expr, ",") <~ ")" ^^ (g => SqlGroupingItem.Cube(g)) |
        "ROLLUP" ~ "(" ~> rep1sep(expr, ",") <~ ")" ^^ (g => SqlGroupingItem.Rollup(g)) |
        "GROUPING" ~ "SETS" ~ "(" ~> rep1sep(expr, ",") <~ ")" ^^ (g => SqlGroupingItem.GroupingSets(g)) |
        expr ^^ (g => SqlGroupingItem.Expr(g))

    def groupBy: Parser[SqlGroupBy] =
        "GROUP" ~ "BY" ~> opt(quantifier) ~ rep1sep(groupingItem, ",") ^^ {
            case q ~ g =>
                SqlGroupBy(q, g)
        }

    def having: Parser[SqlExpr] =
        "HAVING" ~> expr

    def orderBy: Parser[List[SqlOrderingItem]] =
        "ORDER" ~> "BY" ~> rep1sep(order, ",")

    def limit: Parser[Option[SqlLimit]] =
        def create(
            limit: Option[String], 
            offset: Option[String],
            percent: Boolean = false,
            withTies: Boolean = false
        ): Option[SqlLimit] =
            val fetchValue = limit
                .map { v =>
                    val unit = 
                        if percent then SqlFetchUnit.Percentage
                        else SqlFetchUnit.RowCount
                    val mode =
                        if withTies then SqlFetchMode.WithTies
                        else SqlFetchMode.Only
                    SqlFetch(SqlExpr.NumberLiteral(BigDecimal(v)), unit, mode)
                }
            val offsetValue = offset
                .map(v => SqlExpr.NumberLiteral(BigDecimal(v)))
            if fetchValue.isEmpty && offsetValue.isEmpty then None
            else
                Some(SqlLimit(offsetValue, fetchValue))
        "LIMIT" ~> numericLit ~ opt(("OFFSET" | ",") ~ numericLit) ^^ {
            case limit ~ Some("OFFSET" ~ offset) => 
                create(Some(limit), Some(offset))
            case offset ~ Some("," ~ limit) =>
                create(Some(limit), Some(offset))
            case limit ~ _ =>
                create(Some(limit), None)
        } |
        opt("OFFSET" ~> numericLit <~ ("ROW" | "ROWS")) ~
        opt(
            "FETCH" ~ ("FIRST" | "NEXT") ~> numericLit ~ 
            opt("PERCENT") ~ ("ROW" | "ROWS") ~ ("ONLY" | ("WITH" ~ "TIES"))
        ) ^^ {
            case offset ~ fetch =>
                val fetchInfo = fetch.map {
                    case limit ~ percent ~ _ ~ mode =>
                        val withTies = mode match
                            case _ ~ _ => true
                            case _ => false
                        (limit, percent.isDefined, withTies)
                }
                create(
                    fetchInfo.map(_._1),
                    offset,
                    fetchInfo.map(_._2).getOrElse(false),
                    fetchInfo.map(_._3).getOrElse(false)
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
        phrase(query)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw ParseException(e.toString)

class ParseException(msg: String) extends Exception:
    override def toString: String =
        s"sqala.parser.ParseException: \n$msg"
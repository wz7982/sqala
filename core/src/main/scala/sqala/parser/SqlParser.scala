package sqala.parser

import sqala.ast.expr.{SqlBinaryOperator, SqlCase, SqlExpr, SqlSubLinkType}
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.{SqlOrderBy, SqlOrderByOption}
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSelectParam, SqlUnionType}
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTableAlias, SqlTable}

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

class SqlParser extends StandardTokenParsers:
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
           "UNION", "EXCEPT", "INTERSECT", "ALL", "ANY", "EXISTS", "SOME", "COUNT", "SUM", "AVG", "MAX", "MIN"
       )
    )

    lexical.delimiters.addAll(
       List(
           "+", "-", "*", "/", "%", "=", "<>", "!=", ">", ">=", "<", "<=", "(", ")", ",", ".", "`", "\"", "?"
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
                case op ~ n ~ right => (n.isDefined, "is", right)
            } |
            opt("NOT") ~ "BETWEEN" ~ add ~ "AND" ~ add ^^ {
                case n ~ op ~ start ~ _ ~ end => (n.isDefined, "between", start, end)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ select ~ ")" ^^ {
                case n ~ op ~ _ ~ in ~ _ => (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "IN" ~ "(" ~ rep1sep(expr, ",") ~ ")" ^^ {
                case n ~ op ~ _ ~ in ~ _ => (n.isDefined, "in", in)
            } |
            opt("NOT") ~ "LIKE" ~ add ^^ {
                case n ~ op ~ right => (n.isDefined, "like", right)
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
                case (acc, (false, "is", _)) => SqlExpr.Binary(acc, SqlBinaryOperator.Is, SqlExpr.Null)
                case (acc, (true, "is", _)) => SqlExpr.Binary(acc, SqlBinaryOperator.IsNot, SqlExpr.Null)
                case (acc, (not: Boolean, "between", l: SqlExpr, r: SqlExpr)) => SqlExpr.Between(acc, l, r, not)
                case (acc, (false, "in", in: SqlExpr.Binary)) => SqlExpr.Binary(acc, SqlBinaryOperator.In, in)
                case (acc, (true, "in", in: SqlExpr.Binary)) => SqlExpr.Binary(acc, SqlBinaryOperator.NotIn, in)
                case (acc, (false, "in", in: List[?])) => SqlExpr.Binary(acc, SqlBinaryOperator.In, SqlExpr.Vector(in.asInstanceOf[List[SqlExpr]]))
                case (acc, (true, "in", in: List[?])) => SqlExpr.Binary(acc, SqlBinaryOperator.NotIn, SqlExpr.Vector(in.asInstanceOf[List[SqlExpr]]))
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
        windownFunction |
        function |
        aggFunction |
        "(" ~> union <~ ")" |
        column |
        subLink |
        "(" ~> expr <~ ")"

    def function: Parser[SqlExpr] =
        ident ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlExpr.Func(funcName.toUpperCase.nn, args)
        }

    def aggFunc: Parser[String] =
        ("COUNT" | "SUM" | "AVG" | "MAX" | "MIN")

    def aggFunction: Parser[SqlExpr] =
        "COUNT" ~ "(" ~ "*" ~ ")" ^^ (_ => SqlExpr.Func("COUNT", Nil, false, Map(), Nil)) |
        (aggFunc | ident) ~ ("(" ~> repsep(expr, ",") <~ ")") ^^ {
            case funcName ~ args => SqlExpr.Func(funcName.toUpperCase.nn, args, false, Map(), Nil)
        } |
        ident ~ ("(" ~ "DISTINCT" ~> expr <~ ")") ^^ {
            case funcName ~ arg => SqlExpr.Func(funcName.toUpperCase.nn, arg :: Nil, true, Map(), Nil)
        }

    def over: Parser[(List[SqlExpr], List[SqlOrderBy])] =
        "(" ~> "PARTITION" ~> "BY" ~> rep1sep(expr, ",") ~ opt("ORDER" ~> "BY" ~> rep1sep(order, ",")) <~ ")" ^^ {
            case partition ~ order => (partition, order.getOrElse(Nil))
        } |
        "(" ~> "ORDER" ~> "BY" ~> rep1sep(order, ",") <~ ")" ^^ {
            case o => (Nil, o)
        }

    def windownFunction: Parser[SqlExpr] =
        aggFunction ~ "OVER" ~ over ^^ {
            case agg ~ _ ~ o => SqlExpr.Window(agg, o._1, o._2, None)
        }

    def subLink: Parser[SqlExpr] =
        "ANY" ~> "(" ~> union <~ ")" ^^ {
            case u => SqlExpr.SubLink(u.query, SqlSubLinkType.Any)
        } |
        "SOME" ~> "(" ~> union <~ ")" ^^ {
            case u => SqlExpr.SubLink(u.query, SqlSubLinkType.Some)
        } |
        "ALL" ~> "(" ~> union <~ ")" ^^ {
            case u => SqlExpr.SubLink(u.query, SqlSubLinkType.All)
        } |
        "EXISTS" ~> "(" ~> union <~ ")" ^^ {
            case u => SqlExpr.SubLink(u.query, SqlSubLinkType.Exists)
        } |
        "NOT" ~> "EXISTS" ~> "(" ~> union <~ ")" ^^ {
            case u => SqlExpr.SubLink(u.query, SqlSubLinkType.NotExists)
        }

    def order: Parser[SqlOrderBy] =
        expr ~ opt("ASC" | "DESC") ^^ {
            case e ~ Some("DESC") => SqlOrderBy(e, Some(SqlOrderByOption.Desc), None)
            case e ~ _ => SqlOrderBy(e, Some(SqlOrderByOption.Asc), None)
        }

    def cast: Parser[SqlExpr] =
        "CAST" ~> ("(" ~> expr ~ "AS" ~ ident <~ ")") ^^ {
            case expr ~ _ ~ castType => SqlExpr.Cast(expr, castType.toUpperCase.nn)
        }

    def caseWhen: Parser[SqlExpr] =
        "CASE" ~>
            rep1("WHEN" ~> expr ~ "THEN" ~ expr ^^ { case e ~ _ ~ te => SqlCase(e, te) }) ~
            opt("ELSE" ~> expr) <~ "END" ^^ {
                case branches ~ default => SqlExpr.Case(branches, default.getOrElse(SqlExpr.Null))
            }

    def literal: Parser[SqlExpr] =
        numericLit ^^ (i => SqlExpr.NumberLiteral(BigDecimal(i))) |
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
                val param = if distinct.isDefined then Some(SqlSelectParam.Distinct) else None
                SqlExpr.SubQuery(
                    SqlQuery.Select(param, s, f.getOrElse(Nil), w, g.map(_._1.map(i => SqlGroupItem.Singleton(i))).getOrElse(Nil), g.map(_._2).getOrElse(None), o.getOrElse(Nil), l)
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
        ident ~ opt(opt("AS") ~> ident) ^^ {
            case table ~ alias => SqlTable.IdentTable(table, alias.map(a => SqlTableAlias(a, Nil)))
        } |
        opt("LATERAL") ~ ("(" ~> union <~ ")") ~ (opt("AS") ~> ident) ^^ {
            case lateral ~ s ~ alias => SqlTable.SubQueryTable(s.query, lateral.isDefined, SqlTableAlias(alias))
        }

    def joinType: Parser[SqlJoinType] =
        "LEFT" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.LeftJoin) |
        "RIGHT" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.RightJoin) |
        "FULL" ~ opt("OUTER") ~ "JOIN" ^^ (_ => SqlJoinType.FullJoin) |
        "CROSS" ~ "JOIN" ^^ (_ => SqlJoinType.CrossJoin) |
        "INNER" ~ "JOIN" ^^ (_ => SqlJoinType.InnerJoin) |
        "JOIN" ^^ (_ => SqlJoinType.InnerJoin)

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
                case (l, r) => SqlTable.JoinTable(l, r._1, r._2, r._3.map(SqlJoinCondition.On(_)))
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

    def orderBy: Parser[List[SqlOrderBy]] =
        "ORDER" ~> "BY" ~> rep1sep(order, ",")

    def limit: Parser[SqlLimit] =
        "LIMIT" ~ numericLit ~ opt("OFFSET" ~> numericLit) ^^ {
            case _ ~ limit ~ offset => SqlLimit(SqlExpr.NumberLiteral(limit.toInt), SqlExpr.NumberLiteral(offset.map(_.toInt).getOrElse(0)))
        }

    def parseExpr(text: String): SqlExpr =
        phrase(expr)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw ParseException(e.toString)

    def parseColumn(text: String): SqlExpr =
        phrase(column)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw ParseException(e.toString)

    def parseQuery(text: String): SqlQuery =
        phrase(union)(new lexical.Scanner(text)) match
            case Success(result, _) => result.query
            case e => throw ParseException(e.toString)

class ParseException(msg: String) extends Exception:
    override def toString: String =
        s"sqala.parser.ParseException: \n$msg"
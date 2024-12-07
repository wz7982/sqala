package sqala.static.macros

import sqala.ast.expr.*
import sqala.ast.order.*
import sqala.static.common.*
import sqala.static.dsl.*
import sqala.static.statement.query.Query

import scala.quoted.*

object ExprMacro:
    private def binaryOperators: List[String] =
        List(
            "==", "!=", ">", ">=", "<", "<=", "&&", "||",
            "+", "-", "*", "/", "%", "->", "->>",
            "like", "contains", "startsWith", "endsWith", "in"
        )

    private def unaryOperators: List[String] =
        List("unary_+", "unary_-", "unary_!")

    private def missMatch(using q: Quotes)(term: q.reflect.Term): Nothing =
        import q.reflect.*

        report.errorAndAbort(
            s"\"${term.show}\" cannot be converted to SQL expression.", 
            term.asExpr
        )

    private[sqala] def treeInfoMacro(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term
    ): Expr[SqlExpr] =
        import q.reflect.*

        term match
            case TypeApply(
                Select(
                    Apply(
                        Select(ident@Ident(objectName), "selectDynamic"),
                        Literal(StringConstant(valName)) :: Nil
                    ),
                    "$asInstanceOf$"
                ),
                _
            ) =>
                ident.tpe.asType match
                    // TODO 子查询 子查询分为两种单表和命名元组
                    case '[Table[_]] =>
                        val metaDataExpr = ident.tpe.asType match
                            case '[Table[Option[t]]] => TableMacro.tableMetaDataMacro[t]
                            case '[Table[t]] => TableMacro.tableMetaDataMacro[t]
                        val objectNameExpr = Expr(objectName)
                        val valueNameExpr = Expr(valName)
                        val tableNameExpr = 
                            if args.contains(objectName) then
                                val indexExpr = Expr(args.indexOf(objectName))
                                '{ $tableNames($indexExpr) }
                            else objectNameExpr                        
                        '{
                            val columnName = 
                                $metaDataExpr.fieldNames
                                    .zip($metaDataExpr.columnNames)
                                    .find(_._1 == $valueNameExpr)
                                    .map(_._2)
                                    .get
                            SqlExpr.Column(Some($tableNameExpr), columnName)
                        }
            case Apply(Select(left, op), right :: Nil) if binaryOperators.contains(op) =>
                createBinary(args, tableNames, left, op, right)
            case Apply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, tableNames, left, op, right)
            case Apply(Apply(TypeApply(Apply(Ident(op), left :: Nil), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, tableNames, left, op, right)
            case Apply(Apply(TypeApply(Apply(TypeApply(Ident(op), _), left :: Nil), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, tableNames, left, op, right)
            case Apply(Apply(Apply(Ident(op), left :: Nil), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, tableNames, left, op, right)
            case Select(v, op) if unaryOperators.contains(op) =>
                createUnary(args, tableNames, v, op)
            case Apply(Apply(TypeApply(Ident(op), _), v :: Nil), _) if unaryOperators.contains(op) =>
                createUnary(args, tableNames, v, op)
            case Apply(
                Apply(
                    TypeApply(Apply(TypeApply(Ident("between"), _), v :: Nil), _), 
                    s :: e :: Nil
                ), 
                _
            ) =>
                createBetween(args, tableNames, v, s, e)
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms) 
                if t.startsWith("Tuple")
            =>
                createTuple(args, tableNames, terms)
            case Apply(TypeApply(Apply(TypeApply(Ident("as"), _), v :: Nil), _), _ :: _ :: cast :: Nil) =>
                createCast(args, tableNames, v, cast)
            case Apply(Ident("interval"), interval :: Nil) =>
                createInterval(interval)
            case Apply(
                Apply(
                    TypeApply(Ident("extract"), _), 
                    Apply(Apply(TypeApply(Ident(unit), _), v :: Nil), _) :: Nil
                ), 
                _
            ) =>
                createExtract(args, tableNames, unit, v)
            case Apply(
                Apply(
                    TypeApply(Ident("extract"), _), 
                    Apply(TypeApply(Ident(unit), _), v :: Nil) :: Nil
                ), 
                _
            ) =>
                createExtract(args, tableNames, unit, v)
            case Apply(
                Apply(
                    Apply(
                        Ident(s@("timestamp" | "date")), 
                        Apply(_, Typed(Repeated(t :: Nil, _), _) :: Nil) :: Nil
                    ), 
                    _
                ), 
                _
            ) =>
                val timeExpr = t.asExprOf[String]
                if s == "timestamp" then
                    '{ SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, $timeExpr) }
                else
                    '{ SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, $timeExpr) }
            case _ if
                term.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlFunction")), _), _) => true
                    case _ => false
                .isDefined
            =>
                val functionName = 
                    term.symbol.annotations.find:
                        case Apply(Select(New(TypeIdent("sqlFunction")), _), _) => true
                        case _ => false
                    .get match
                        case Apply(Select(New(TypeIdent("sqlFunction")), _), functionArg :: Nil) =>
                            functionArg match
                                case Literal(StringConstant(n)) => n
                                case NamedArg(_, Literal(StringConstant(n))) => n
                                case _ => missMatch(term)
                        case _ => missMatch(term)
                createFunction(args, tableNames, functionName, term)
            // TODO 自定义二元运算
            case Apply(
                Apply(Apply(TypeApply(Ident("over"), _), function :: Nil), overValue :: Nil),
                _
            ) =>
                createOver(args, tableNames, function, overValue)
            case Apply(TypeApply(Select(Ident("Some"), "apply"), _), v :: Nil) =>
                treeInfoMacro(args, tableNames, v)
            case Apply(Ident(name), v :: Nil) if name.endsWith("2bigDecimal") =>
                treeInfoMacro(args, tableNames, v)
            case i@If(_, _, _) =>
                createIf(args, tableNames, i)
            case m@Match(_, _) =>
                createMatch(args, tableNames, m)
            case Apply(Apply(Ident("exists"), q :: Nil), _) =>
                val expr = q.asExprOf[Query[?]]
                '{ SqlExpr.SubLink($expr.ast, SqlSubLinkType.Exists) }
            case Apply(Apply(Ident("any"), q :: Nil), _) =>
                val expr = q.asExprOf[Query[?]]
                '{ SqlExpr.SubLink($expr.ast, SqlSubLinkType.Any) }
            case Apply(Apply(Ident("all"), q :: Nil), _) =>
                val expr = q.asExprOf[Query[?]]
                '{ SqlExpr.SubLink($expr.ast, SqlSubLinkType.All) }
            case _ =>
                term.tpe.asType match
                    case '[Query[_]] =>
                        val expr = term.asExprOf[Query[?]]
                        '{ SqlExpr.SubQuery($expr.ast) }
                    case _ => createValue(term)

    private def createBinary(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        left: q.reflect.Term,
        op: String,
        right: q.reflect.Term
    ): Expr[SqlExpr] =
        val leftExpr = treeInfoMacro(args, tableNames, left)
        val leftHasString = left.tpe.widen.asType match
            case '[String] => true
            case '[Option[String]] => true
            case _ => false

        val rightExpr = treeInfoMacro(args, tableNames, right)
        val rightHasString = right.tpe.widen.asType match
            case '[String] => true
            case '[Option[String]] => true
            case _ => false

        val operatorExpr = op match
            case "==" => '{ SqlBinaryOperator.Equal }
            case "!=" => '{ SqlBinaryOperator.NotEqual }
            case ">" => '{ SqlBinaryOperator.GreaterThan }
            case ">=" => '{ SqlBinaryOperator.GreaterThanEqual }
            case "<" => '{ SqlBinaryOperator.LessThan }
            case "<=" => '{ SqlBinaryOperator.LessThanEqual }
            case "&&" => '{ SqlBinaryOperator.And }
            case "||" => '{ SqlBinaryOperator.Or }
            case "+" => '{ SqlBinaryOperator.Plus }
            case "-" => '{ SqlBinaryOperator.Minus }
            case "*" => '{ SqlBinaryOperator.Times }
            case "/" => '{ SqlBinaryOperator.Div }
            case "%" => '{ SqlBinaryOperator.Mod }
            case "->" => '{ SqlBinaryOperator.Json }
            case "->>" => '{ SqlBinaryOperator.JsonText }
            case "like" => '{ SqlBinaryOperator.Like }
            case "contains" => '{ SqlBinaryOperator.Like }
            case "startsWith" => '{ SqlBinaryOperator.Like }
            case "endsWith" => '{ SqlBinaryOperator.Like }
            case "in" => '{ SqlBinaryOperator.In }

        val opNameExpr = Expr(op)

        if op == "+" && (leftHasString || rightHasString) then
            '{
                SqlExpr.Func("CONCAT", $leftExpr :: $rightExpr :: Nil) 
            }
        else
            '{
                ($leftExpr, $operatorExpr, $rightExpr, $opNameExpr) match
                    case (l, SqlBinaryOperator.Equal, SqlExpr.Null, _) =>
                        SqlExpr.NullTest(l, false)
                    case (l, SqlBinaryOperator.NotEqual, SqlExpr.Null, _) =>
                        SqlExpr.NullTest(l, true)
                    case (l, SqlBinaryOperator.NotEqual, r, _) =>
                        SqlExpr.Binary(
                            SqlExpr.Binary(l, SqlBinaryOperator.NotEqual, r),
                            SqlBinaryOperator.Or,
                            SqlExpr.NullTest(l, false)
                        )
                    case (l, SqlBinaryOperator.Like, SqlExpr.StringLiteral(s), "contains") =>
                        SqlExpr.Binary(l, SqlBinaryOperator.Like, SqlExpr.StringLiteral("%" + s + "%"))
                    case (l, SqlBinaryOperator.Like, r, "contains") =>
                        SqlExpr.Binary(
                            l,
                            SqlBinaryOperator.Like,
                            SqlExpr.Func("CONCAT", SqlExpr.StringLiteral("%") :: r :: SqlExpr.StringLiteral("%") :: Nil)
                        )
                    case (l, SqlBinaryOperator.Like, SqlExpr.StringLiteral(s), "startsWith") =>
                        SqlExpr.Binary(l, SqlBinaryOperator.Like, SqlExpr.StringLiteral(s + "%"))
                    case (l, SqlBinaryOperator.Like, r, "startsWith") =>
                        SqlExpr.Binary(
                            l,
                            SqlBinaryOperator.Like,
                            SqlExpr.Func("CONCAT", r :: SqlExpr.StringLiteral("%") :: Nil)
                        )
                    case (l, SqlBinaryOperator.Like, SqlExpr.StringLiteral(s), "endsWith") =>
                        SqlExpr.Binary(l, SqlBinaryOperator.Like, SqlExpr.StringLiteral("%" + s))
                    case (l, SqlBinaryOperator.Like, r, "endsWith") =>
                        SqlExpr.Binary(
                            l,
                            SqlBinaryOperator.Like,
                            SqlExpr.Func("CONCAT", SqlExpr.StringLiteral("%") :: r :: Nil)
                        )
                    case (l, SqlBinaryOperator.In, SqlExpr.Vector(Nil), _) =>
                        SqlExpr.BooleanLiteral(false)
                    case (l, o, r, _) =>
                        SqlExpr.Binary(l, o, r)
            }

    private def createUnary(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        op: String
    ): Expr[SqlExpr] =
        val rightExpr = treeInfoMacro(args, tableNames, term)

        val operatorExpr = op match
            case "unary_+" => '{ SqlUnaryOperator.Positive }
            case "unary_-" => '{ SqlUnaryOperator.Negative }
            case "unary_!" => '{ SqlUnaryOperator.Not }

        '{
            ($operatorExpr, $rightExpr) match
                case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.Like, r)) =>
                    SqlExpr.Binary(l, SqlBinaryOperator.NotLike, r)
                case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.In, SqlExpr.Vector(Nil))) =>
                    SqlExpr.BooleanLiteral(true)
                case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.In, r)) =>
                    SqlExpr.Binary(l, SqlBinaryOperator.NotIn, r)
                case (SqlUnaryOperator.Not, SqlExpr.Between(x, s, e, false)) =>
                    SqlExpr.Between(x, s, e, true)
                case (SqlUnaryOperator.Not, SqlExpr.SubLink(q, SqlSubLinkType.Exists)) =>
                    SqlExpr.SubLink(q, SqlSubLinkType.NotExists)
                case (SqlUnaryOperator.Not, SqlExpr.BooleanLiteral(false)) =>
                    SqlExpr.BooleanLiteral(true)
                case (SqlUnaryOperator.Not, SqlExpr.BooleanLiteral(true)) =>
                    SqlExpr.BooleanLiteral(false)
                case (o, x) =>
                    SqlExpr.Unary(x, o)
        }

    private def createBetween(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        startTerm: q.reflect.Term,
        endTerm: q.reflect.Term
    ): Expr[SqlExpr] =
        val betweenExpr = treeInfoMacro(args, tableNames, term)
        val startExpr = treeInfoMacro(args, tableNames, startTerm)
        val endExpr = treeInfoMacro(args, tableNames, endTerm)

        '{
            SqlExpr.Between($betweenExpr, $startExpr, $endExpr, false)
        }

    private def createTuple(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        terms: List[q.reflect.Term]
    ): Expr[SqlExpr] =
        val exprs = terms
            .map(t => treeInfoMacro(args, tableNames, t))
        val listExpr = Expr.ofList(exprs)
        '{
            SqlExpr.Vector($listExpr)
        }

    private def createCast(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        cast: q.reflect.Term
    ): Expr[SqlExpr] =
        cast.tpe.asType match
            case '[Cast[_, _]] =>
                val castExpr = cast.asExprOf[Cast[?, ?]]
                val expr = treeInfoMacro(args, tableNames, term)
                '{
                    SqlExpr.Cast($expr, $castExpr.castType)
                }

    private def createInterval(using q: Quotes)(term: q.reflect.Term): Expr[SqlExpr] =
        term.tpe.asType match
            case '[TimeInterval] =>
                val expr = term.asExprOf[TimeInterval]
                '{ 
                   SqlExpr.Interval($expr.n, $expr.unit)
                }

    private def createExtract(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        unit: String,
        term: q.reflect.Term
    ): Expr[SqlExpr] =
        val unitExpr = unit match
            case "year" => '{ SqlTimeUnit.Year }
            case "month" => '{ SqlTimeUnit.Month }
            case "week" => '{ SqlTimeUnit.Week }
            case "day" => '{ SqlTimeUnit.Day }
            case "hour" => '{ SqlTimeUnit.Hour }
            case "minute" => '{ SqlTimeUnit.Minute }
            case "second" => '{ SqlTimeUnit.Second }
            case _ => missMatch(term)
        val expr = treeInfoMacro(args, tableNames, term)
        '{
            SqlExpr.Extract($unitExpr, $expr)
        }

    private def removeNestedApply(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
        import q.reflect.*

        term match
            case Apply(a@Apply(_, _), _) => a
            case _ => term

    private def createFunction(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        name: String,
        term: q.reflect.Term
    ): Expr[SqlExpr] =
        import q.reflect.*

        val paramTerms = removeNestedApply(term) match
            case Apply(TypeApply(_, _), p) => p
            case Apply(_, p) => p

        val symbol = term.symbol
        val params = symbol.paramSymss.flatten.filter(p => p.isTerm).filter: p =>
            p.flags match
                case f if f.is(Flags.Given) => false
                case _ => true
        
        val paramNames = params.map(_.name)

        val namedParams = paramTerms.filter:
            case NamedArg(n, p) => true
            case _ => false
        .map:
            case NamedArg(n, p) => (n, p)

        val unnamedParamNames = paramNames.filter(n => !namedParams.map(_._1).contains(n))
        val unnamedParams = paramTerms.filter:
            case NamedArg(n, p) => false
            case _ => true

        val allParams = namedParams ++ (unnamedParamNames.zip(unnamedParams))

        val valueParams = allParams
            .filter(p => !List("sortBy", "withinGroup").contains(p._1))
            .map(_._2)

        val distinct = symbol.name.endsWith("Distinct")

        val sortByParam = allParams.find(_._1 == "sortBy").map(_._2)

        val withinGroupParam = allParams.find(_._1 == "withinGroup").map(_._2)

        val valueParamInfo = valueParams
            .map(p => treeInfoMacro(args, tableNames, p))
        val valueParamExpr = Expr.ofList(valueParamInfo)

        val sortByList = sortByParam.toList.map:
            case Typed(Repeated(x, _), _) => x
            case x => x :: Nil
        .flatten
        val sortByInfo = sortByList
            .map(s => sortInfoMacro(args, tableNames, s))
        val sortByExpr = Expr.ofList(sortByInfo)

        val withinGroupList = withinGroupParam.toList.map:
            case Typed(Repeated(x, _), _) => x
            case x => x :: Nil
        .flatten
        val withinGroupInfo = withinGroupList
            .map(s => sortInfoMacro(args, tableNames, s))
        val withinGroupExpr = Expr.ofList(withinGroupInfo)

        val nameExpr = Expr(name)
        val distinctExpr = Expr(distinct)

        '{
            SqlExpr.Func(
                $nameExpr, 
                $valueParamExpr, 
                $distinctExpr, 
                $sortByExpr,
                $withinGroupExpr, 
                None
            )
        }

    private def createOver(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        overValue: q.reflect.Term
    ): Expr[SqlExpr] =
        import q.reflect.*

        val functionExpr = treeInfoMacro(args, tableNames, term)

        def splitFrame(value: Term): (Term, Option[(String, Term, Term)]) =
            value match
                case Apply(
                    Apply(
                        Select(t, kind@("rowsBetween" | "rangeBetween" | "groupsBetween")), 
                        s :: e :: Nil
                    ), 
                    _
                ) =>
                    (t, Some(kind, s, e))
                case _ => (value, None)

        def fetchFrameOption(value: Term): Expr[SqlWindowFrameOption] =
            value match
                case Ident("currentRow") => '{ SqlWindowFrameOption.CurrentRow }
                case Ident("unboundedPreceding") => '{ SqlWindowFrameOption.UnboundedPreceding }
                case Ident("unboundedFollowing") => '{ SqlWindowFrameOption.UnboundedFollowing }
                case Apply(Ident("preceding"), n :: Nil) =>
                    val nExpr = n.asExprOf[Int]
                    '{ SqlWindowFrameOption.Preceding($nExpr) }
                case Apply(Ident("following"), n :: Nil) =>
                    val nExpr = n.asExprOf[Int]
                    '{ SqlWindowFrameOption.Following($nExpr) }
                case _ => missMatch(term)

        val (value, frame) = splitFrame(overValue)

        val frameExpr: Expr[Option[SqlWindowFrame]] =
            frame match
                case None => '{ None }
                case Some(f) =>
                    val startExpr = fetchFrameOption(f._2)
                    val endExpr = fetchFrameOption(f._3)
                    f._1 match
                        case "rowsBetween" => '{ Some(SqlWindowFrame.Rows($startExpr, $endExpr)) }
                        case "rangeBetween" => '{ Some(SqlWindowFrame.Range($startExpr, $endExpr)) }
                        case "groupsBetween" => '{ Some(SqlWindowFrame.Groups($startExpr, $endExpr)) }

        val (partition, sort) = value match 
            case Apply(Apply(Ident("partitionBy"), Typed(Repeated(partitionBy, _), _) :: Nil), _) =>
                partitionBy.map(p => treeInfoMacro(args, tableNames, p)) ->
                Nil
            case Apply(Apply(Ident("sortBy"), Typed(Repeated(sortBy, _), _) :: Nil), _) =>
                Nil ->
                sortBy.map(s => sortInfoMacro(args, tableNames, s))
            case Apply(
                Apply(
                    Select(
                        Apply(Apply(Ident("partitionBy"), Typed(Repeated(partitionBy, _), _) :: Nil), _),
                        "sortBy"
                    ),
                    Typed(Repeated(sortBy, _), _) :: Nil
                ),
                _
            ) =>
                partitionBy.map(p => treeInfoMacro(args, tableNames, p)) ->
                sortBy.map(s => sortInfoMacro(args, tableNames, s))
            case _ => Nil -> Nil

        val partitionExpr = Expr.ofList(partition)
        val sortExpr = Expr.ofList(sort)

        '{
            SqlExpr.Window(
                $functionExpr, 
                $partitionExpr, 
                $sortExpr, 
                $frameExpr
            )
        }

    private def createIf(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term
    ): Expr[SqlExpr] =
        import q.reflect.*

        def collectIf(ifTerm: q.reflect.Term, exprs: List[Expr[SqlExpr]]): List[Expr[SqlExpr]] =
            ifTerm match
                case If(i, t, e) =>
                    val newExprs = exprs ++ List(
                        treeInfoMacro(args, tableNames, i),
                        treeInfoMacro(args, tableNames, t)
                    )
                    collectIf(e, newExprs)
                case t => exprs :+ treeInfoMacro(args, tableNames, t)

        val expr = Expr.ofList(collectIf(term, Nil))

        '{
            val branches = $expr.dropRight(1).grouped(2).toList.map: b =>
                SqlCase(b(0), b(1))
            SqlExpr.Case(branches, $expr.takeRight(1).head)
        }

    private def createMatch(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term
    ): Expr[SqlExpr] =
        import q.reflect.*

        term match
            case Match(m, c) =>
                val matchExpr = treeInfoMacro(args, tableNames, m)
                val caseExprs = c.map:
                    case CaseDef(Ident(_), _, Block(_, v)) =>
                        val valueExpr = treeInfoMacro(args, tableNames, v)
                        '{ SqlCase($matchExpr, $valueExpr) }
                    case CaseDef(c, _, Block(_, v)) =>
                        val caseExpr = treeInfoMacro(args, tableNames, c.asExpr.asTerm)
                        val valueExpr = treeInfoMacro(args, tableNames, v)
                        '{ SqlCase($caseExpr, $valueExpr) }
                    case _ => missMatch(term)
                val caseExpr = Expr.ofList(caseExprs)
                '{ 
                    val branches = $caseExpr
                        .filter(c => c.whenExpr != $matchExpr)
                    val default = $caseExpr
                        .find(c => c.whenExpr == $matchExpr)
                        .map(_.thenExpr)
                        .getOrElse(SqlExpr.Null)
                    SqlExpr.Match($matchExpr, branches, default) 
                }

    private def createValue(using q: Quotes)(term: q.reflect.Term): Expr[SqlExpr] =
        term.tpe.widen.asType match
            case '[Seq[t]] =>
                val asSqlExpr = Expr.summon[Merge[t]]
                asSqlExpr match
                    case None => missMatch(term)
                    case Some(e) =>
                        val valueExpr = term.asExprOf[Seq[t]]
                        '{ SqlExpr.Vector($valueExpr.map(i => $e.asSqlExpr(i)).toList) }
            case '[t] =>
                val asSqlExpr = Expr.summon[AsSqlExpr[t]]
                asSqlExpr match
                    case None => missMatch(term)
                    case Some(e) =>
                        val valueExpr = term.asExprOf[t]
                        '{ $e.asSqlExpr($valueExpr) }

    private[sqala] def sortInfoMacro(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
    ): Expr[SqlOrderBy] =
        import q.reflect.*
        
        term match
            case Apply(Apply(TypeApply(Ident(op), _), v :: Nil), _) =>
                val expr = treeInfoMacro(args, tableNames, v)
                op match
                    case "asc" => '{
                        SqlOrderBy($expr, Some(SqlOrderByOption.Asc), None)
                    }
                    case "ascNullsFirst" => '{
                        SqlOrderBy($expr, Some(SqlOrderByOption.Asc), Some(SqlOrderByNullsOption.First))
                    }
                    case "ascNullsLast" => '{
                        SqlOrderBy($expr, Some(SqlOrderByOption.Asc), Some(SqlOrderByNullsOption.Last))
                    }
                    case "desc" => '{
                        SqlOrderBy($expr, Some(SqlOrderByOption.Desc), None)
                    }
                    case "descNullsFirst" => '{
                        SqlOrderBy($expr, Some(SqlOrderByOption.Desc), Some(SqlOrderByNullsOption.First))
                    }
                    case "descNullsLast" => '{
                        SqlOrderBy($expr, Some(SqlOrderByOption.Desc), Some(SqlOrderByNullsOption.Last))
                    }
                    case _ => missMatch(term)
            case _ =>
                val expr = treeInfoMacro(args, tableNames, term)
                '{ SqlOrderBy($expr, Some(SqlOrderByOption.Asc), None) }
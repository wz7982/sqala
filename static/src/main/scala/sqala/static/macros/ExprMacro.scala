package sqala.static.macros

import sqala.ast.expr.*
import sqala.ast.order.*
import sqala.ast.param.SqlParam
import sqala.static.common.*
import sqala.static.dsl.*
import sqala.static.statement.query.*

import scala.quoted.*

object ExprMacro:
    case class ExprInfo(
        hasAgg: Boolean,
        isAgg: Boolean,
        isGroup: Boolean,
        hasWindow: Boolean,
        isConst: Boolean,
        columnRef: List[(String, String)],
        aggRef: List[(String, String)],
        nonAggRef: List[(String, String)],
        ungroupedRef: List[(String, String)]
    )

    private[sqala] def binaryOperators: List[String] =
        List(
            "==", "!=", ">", ">=", "<", "<=", "&&", "||",
            "+", "-", "*", "/", "%", "->", "->>",
            "like", "contains", "startsWith", "endsWith"
        )

    private[sqala] def isBinaryOperator(using q: Quotes)(name: String, term: q.reflect.Term): Boolean =
        import q.reflect.*

        binaryOperators.contains(name) ||
        term.symbol.annotations.exists:
            case Apply(Select(New(TypeIdent("sqlBinaryOperator")), _), _) => true
            case _ => false

    private[sqala] def fetchBinaryOperator(using q: Quotes)(name: String, term: q.reflect.Term): String =
        import q.reflect.*

        term.symbol.annotations.find:
            case Apply(Select(New(TypeIdent("sqlBinaryOperator")), _), _) => true
            case _ => false
        .map:
            case Apply(Select(New(TypeIdent("sqlBinaryOperator")), _), n :: Nil) =>
                n match
                    case Literal(StringConstant(n)) => n
                    case NamedArg(_, Literal(StringConstant(n))) => n
        .getOrElse(name)

    private[sqala] def unaryOperators: List[String] =
        List("unary_+", "unary_-", "unary_!", "prior")

    private[sqala] def missMatch(using q: Quotes)(term: q.reflect.Term): Nothing =
        import q.reflect.*

        report.errorAndAbort(
            s"\"${term.show}\" cannot be converted to SQL expression.",
            term.asExpr
        )

    private[sqala] def validateDiv(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        val isZero = term match
            case Literal(IntConstant(0)) => true
            case Literal(LongConstant(0L)) => true
            case Literal(FloatConstant(0F)) => true
            case Literal(DoubleConstant(0D)) => true
            case _ => false
        if isZero then report.error("Division by zero.", term.asExpr)

    private[sqala] def validateQuery(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        term.tpe.asType match
            case '[Query[_, ManyRows]] =>
                report.error("Subquery must return only one row.", term.asExpr)
            case _ =>

    private[sqala] def treeInfoMacro(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
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
                    case '[Table[_]] =>
                        val metaDataExpr = ident.tpe.asType match
                            case '[Table[Option[t]]] => TableMacro.tableMetaDataMacro[t]
                            case '[Table[t]] => TableMacro.tableMetaDataMacro[t]
                        val objectNameExpr = Expr(objectName)
                        val valueNameExpr = Expr(valName)
                        val tableNameExpr =
                            if isPrior then
                                Expr(tableCte)
                            else if args.contains(objectName) then
                                val indexExpr = Expr(args.indexOf(objectName))
                                '{ $tableNames($indexExpr) }
                            else objectNameExpr
                        val sqlExpr = '{
                            val columnName =
                                $metaDataExpr.fieldNames
                                    .zip($metaDataExpr.columnNames)
                                    .find(_._1 == $valueNameExpr)
                                    .map(_._2)
                                    .get
                            SqlExpr.Column(Some($tableNameExpr), columnName)
                        }
                        val info = ExprInfo(
                            hasAgg = false,
                            isAgg = false,
                            isGroup = false,
                            hasWindow = false,
                            isConst = false,
                            columnRef = (objectName, valName) :: Nil,
                            aggRef = Nil,
                            nonAggRef = (objectName, valName) :: Nil,
                            ungroupedRef = Nil
                        )
                        sqlExpr -> info
                    case '[UngroupedTable[_]] =>
                        val metaDataExpr = ident.tpe.asType match
                            case '[UngroupedTable[Option[t]]] => TableMacro.tableMetaDataMacro[t]
                            case '[UngroupedTable[t]] => TableMacro.tableMetaDataMacro[t]
                        val objectNameExpr = Expr(objectName)
                        val valueNameExpr = Expr(valName)
                        val tableNameExpr =
                            if args.contains(objectName) then
                                val indexExpr = Expr(args.indexOf(objectName))
                                '{ $tableNames($indexExpr) }
                            else objectNameExpr
                        val sqlExpr = '{
                            val columnName =
                                $metaDataExpr.fieldNames
                                    .zip($metaDataExpr.columnNames)
                                    .find(_._1 == $valueNameExpr)
                                    .map(_._2)
                                    .get
                            SqlExpr.Column(Some($tableNameExpr), columnName)
                        }
                        if inDistinctOn then
                            val info = ExprInfo(
                                hasAgg = true,
                                isAgg = true,
                                isGroup = false,
                                hasWindow = false,
                                isConst = false,
                                columnRef = (objectName, valName) :: Nil,
                                aggRef = (objectName, valName) :: Nil,
                                nonAggRef = Nil,
                                ungroupedRef = Nil
                            )
                            '{ SqlExpr.Func("ANY_VALUE", $sqlExpr :: Nil) } -> info
                        else
                            val info = ExprInfo(
                                hasAgg = false,
                                isAgg = false,
                                isGroup = false,
                                hasWindow = false,
                                isConst = false,
                                columnRef = (objectName, valName) :: Nil,
                                aggRef = Nil,
                                nonAggRef = (objectName, valName) :: Nil,
                                ungroupedRef = (objectName, valName) :: Nil
                            )
                            sqlExpr -> info
                    case '[Group[n, _]] =>
                        val objectNameExpr = Expr(objectName)
                        val valueNameExpr = Expr(valName)
                        val tpe = TypeRepr.of[n]
                        val colNames = tpe match
                            case AppliedType(_, names) =>
                                names.map:
                                    case ConstantType(StringConstant(n)) => Expr(n)
                        val namesExpr = Expr.ofList(colNames)
                        val sqlExpr = '{
                            val group =
                                $queryContext.groups.find(_._1 == $objectNameExpr).get
                            val index = $namesExpr.indexOf($valueNameExpr)
                            group._2.apply(index)
                        }
                        val info = ExprInfo(
                            hasAgg = false,
                            isAgg = false,
                            isGroup = true,
                            hasWindow = false,
                            isConst = false,
                            columnRef = (objectName, valName) :: Nil,
                            aggRef = Nil,
                            nonAggRef = (objectName, valName) :: Nil,
                            ungroupedRef = Nil
                        )
                        sqlExpr -> info
                    case '[SubQuery[_, _]] =>
                        val objectNameExpr = Expr(objectName)
                        val valueNameExpr = Expr(valName)
                        val tableNameExpr =
                            if isPrior then
                                Expr(tableCte)
                            else if args.contains(objectName) then
                                val indexExpr = Expr(args.indexOf(objectName))
                                '{ $tableNames($indexExpr) }
                            else objectNameExpr
                        val sqlExpr = '{ SqlExpr.Column(Some($tableNameExpr), $valueNameExpr) }
                        val info = ExprInfo(
                            hasAgg = false,
                            isAgg = false,
                            isGroup = false,
                            hasWindow = false,
                            isConst = false,
                            columnRef = (objectName, valName) :: Nil,
                            aggRef = Nil,
                            nonAggRef = (objectName, valName) :: Nil,
                            ungroupedRef = Nil
                        )
                        sqlExpr -> info
                    case '[UngroupedSubQuery[_, _]] =>
                        val objectNameExpr = Expr(objectName)
                        val valueNameExpr = Expr(valName)
                        val tableNameExpr =
                            if args.contains(objectName) then
                                val indexExpr = Expr(args.indexOf(objectName))
                                '{ $tableNames($indexExpr) }
                            else objectNameExpr
                        val sqlExpr = '{ SqlExpr.Column(Some($tableNameExpr), $valueNameExpr) }
                        if inDistinctOn then
                            val info = ExprInfo(
                                hasAgg = true,
                                isAgg = true,
                                isGroup = false,
                                hasWindow = false,
                                isConst = false,
                                columnRef = (objectName, valName) :: Nil,
                                aggRef = (objectName, valName) :: Nil,
                                nonAggRef = Nil,
                                ungroupedRef = Nil
                            )
                            '{ SqlExpr.Func("ANY_VALUE", $sqlExpr :: Nil) } -> info
                        else
                            val info = ExprInfo(
                                hasAgg = false,
                                isAgg = false,
                                isGroup = false,
                                hasWindow = false,
                                isConst = false,
                                columnRef = (objectName, valName) :: Nil,
                                aggRef = Nil,
                                nonAggRef = (objectName, valName) :: Nil,
                                ungroupedRef = (objectName, valName) :: Nil
                            )
                            sqlExpr -> info
                    case _ => missMatch(term)
            case Apply(Select(left, op), right :: Nil) if isBinaryOperator(op, term) =>
                val operator = fetchBinaryOperator(op, term)
                createBinary(args, tableNames, left, operator, right, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), right :: Nil), _)
                if isBinaryOperator(op, term)
            =>
                val operator = fetchBinaryOperator(op, term)
                createBinary(args, tableNames, left, operator, right, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(TypeApply(Apply(Ident(op), left :: Nil), _), right :: Nil), _)
                if isBinaryOperator(op, term)
            =>
                val operator = fetchBinaryOperator(op, term)
                createBinary(args, tableNames, left, operator, right, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(TypeApply(Apply(TypeApply(Ident(op), _), left :: Nil), _), right :: Nil), _)
                if isBinaryOperator(op, term)
            =>
                val operator = fetchBinaryOperator(op, term)
                createBinary(args, tableNames, left, operator, right, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(Apply(Ident(op), left :: Nil), right :: Nil), _)
                if isBinaryOperator(op, term)
            =>
                val operator = fetchBinaryOperator(op, term)
                createBinary(args, tableNames, left, operator, right, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Select(v, op) if unaryOperators.contains(op) =>
                createUnary(args, tableNames, v, op, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(TypeApply(Ident(op), _), v :: Nil), _) if unaryOperators.contains(op) =>
                createUnary(args, tableNames, v, op, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(
                Apply(
                    TypeApply(Apply(TypeApply(Ident("between"), _), v :: Nil), _),
                    s :: e :: Nil
                ),
                _
            ) =>
                createBetween(args, tableNames, v, s, e, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(TypeApply(Apply(TypeApply(Ident("in"), _), e :: Nil), _), i :: Nil), _) =>
                createIn(args, tableNames, e, i, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(TypeApply(Select(Ident(t), "apply"), _), terms)
                if t.startsWith("Tuple")
            =>
                createTuple(args, tableNames, terms, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(TypeApply(Apply(TypeApply(Ident("as"), _), v :: Nil), _), _ :: _ :: cast :: Nil) =>
                createCast(args, tableNames, v, cast, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Ident("interval"), interval :: Nil) =>
                createInterval(interval)
            case Apply(
                Apply(
                    TypeApply(Ident("extract"), _),
                    Apply(Apply(TypeApply(Ident(unit), _), v :: Nil), _) :: Nil
                ),
                _
            ) =>
                createExtract(args, tableNames, unit, v, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(
                Apply(
                    TypeApply(Ident("extract"), _),
                    Apply(TypeApply(Ident(unit), _), v :: Nil) :: Nil
                ),
                _
            ) =>
                createExtract(args, tableNames, unit, v, queryContext, inConnectBy, isPrior, inDistinctOn)
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
                val sqlExpr = if s == "timestamp" then
                    '{ SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Timestamp, $timeExpr) }
                else
                    '{ SqlExpr.TimeLiteral(SqlTimeLiteralUnit.Date, $timeExpr) }
                sqlExpr -> ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = true,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
            case _ if
                term.symbol.annotations.find:
                    case Apply(Select(New(TypeIdent("sqlFunction" | "sqlAgg" | "sqlWindow")), _), _) => true
                    case _ => false
                .isDefined
            =>
                val (functionName, kind) =
                    term.symbol.annotations.find:
                        case Apply(Select(New(TypeIdent("sqlFunction" | "sqlAgg" | "sqlWindow")), _), _) => true
                        case _ => false
                    .get match
                        case Apply(
                            Select(New(TypeIdent(k)), _),
                            functionArg :: Nil
                        ) =>
                            functionArg match
                                case Literal(StringConstant(n)) => n -> k
                                case NamedArg(_, Literal(StringConstant(n))) => n -> k
                                case _ => missMatch(term)
                        case _ => missMatch(term)
                (functionName, kind) match
                    case ("GROUPING", _) =>
                        createGrouping(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)
                    case (_, "sqlAgg") =>
                        createAgg(args, tableNames, functionName, term, queryContext, inConnectBy, isPrior, inDistinctOn)
                    case (_, "sqlFunction") =>
                        createFunction(args, tableNames, functionName, term, false, queryContext, inConnectBy, isPrior, inDistinctOn)
                    case (_, "sqlWindow") =>
                        createFunction(args, tableNames, functionName, term, false, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(
                Apply(Apply(TypeApply(Ident("over"), _), function :: Nil), overValue :: Nil),
                _
            ) =>
                createOver(args, tableNames, function, overValue, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(TypeApply(Select(Ident("Some"), "apply"), _), v :: Nil) =>
                treeInfoMacro(args, tableNames, v, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Ident(name), v :: Nil) if name.endsWith("2bigDecimal") =>
                treeInfoMacro(args, tableNames, v, queryContext, inConnectBy, isPrior, inDistinctOn)
            case i@If(_, _, _) =>
                createIf(args, tableNames, i, queryContext, inConnectBy, isPrior, inDistinctOn)
            case m@Match(_, _) =>
                createMatch(args, tableNames, m, queryContext, inConnectBy, isPrior, inDistinctOn)
            case Apply(Apply(Ident("exists"), q :: Nil), _) =>
                val expr = q.asExprOf[Query[?, ?]]
                val sqlExpr = '{ SqlExpr.SubLink($expr.ast, SqlSubLinkType.Exists) }
                sqlExpr -> ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
            case Apply(Apply(TypeApply(Ident("any"), _), q :: Nil), _) =>
                val sqlExpr = q.tpe.asType match
                    case '[Query[?, ?]] =>
                        val expr = q.asExprOf[Query[?, ?]]
                        '{ SqlExpr.SubLink($expr.ast, SqlSubLinkType.Any) }
                    case _ =>
                        val (expr, _) = treeInfoMacro(args, tableNames, q, queryContext, inConnectBy, isPrior, inDistinctOn)
                        '{ SqlExpr.Func("ANY", $expr :: Nil) }
                sqlExpr -> ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
            case Apply(Apply(TypeApply(Ident("all"), _), q :: Nil), _) =>
                val sqlExpr = q.tpe.asType match
                    case '[Query[?, ?]] =>
                        val expr = q.asExprOf[Query[?, ?]]
                        '{ SqlExpr.SubLink($expr.ast, SqlSubLinkType.All) }
                    case _ =>
                        val (expr, _) = treeInfoMacro(args, tableNames, q, queryContext, inConnectBy, isPrior, inDistinctOn)
                        '{ SqlExpr.Func("ANY", $expr :: Nil) }
                sqlExpr -> ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
            case _ =>
                term.tpe.asType match
                    case '[Query[_, _]] =>
                        validateQuery(term)
                        val expr = term.asExprOf[Query[?, ?]]
                        val sqlExpr = '{ SqlExpr.SubQuery($expr.ast) }
                        sqlExpr -> ExprInfo(
                            hasAgg = false,
                            isAgg = false,
                            isGroup = false,
                            hasWindow = false,
                            isConst = false,
                            columnRef = Nil,
                            aggRef = Nil,
                            nonAggRef = Nil,
                            ungroupedRef = Nil
                        )
                    case _ => createValue(term)

    private[sqala] def createBinary(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        left: q.reflect.Term,
        op: String,
        right: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        if op == "/" || op == "%" then
            validateDiv(right)

        val (leftExpr, leftInfo) = treeInfoMacro(args, tableNames, left, queryContext, inConnectBy, isPrior, inDistinctOn)
        val leftHasString = left.tpe.widen.asType match
            case '[String] => true
            case '[Option[String]] => true
            case _ => false

        val (rightExpr, rightInfo) = treeInfoMacro(args, tableNames, right, queryContext, inConnectBy, isPrior, inDistinctOn)
        val rightHasString = right.tpe.widen.asType match
            case '[String] => true
            case '[Option[String]] => true
            case _ => false

        val opNameExpr = Expr(op)

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
            case n => '{ SqlBinaryOperator.Custom($opNameExpr) }

        val sqlExpr = if op == "+" && (leftHasString || rightHasString) then
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
                    case (l, o, r, _) =>
                        SqlExpr.Binary(l, o, r)
            }

        sqlExpr -> ExprInfo(
            hasAgg = leftInfo.hasAgg || rightInfo.hasAgg,
            isAgg = false,
            isGroup = false,
            hasWindow = leftInfo.hasWindow || rightInfo.hasWindow,
            isConst = false,
            columnRef = leftInfo.columnRef ++ rightInfo.columnRef,
            aggRef = leftInfo.aggRef ++ rightInfo.aggRef,
            nonAggRef = leftInfo.nonAggRef ++ rightInfo.nonAggRef,
            ungroupedRef = leftInfo.ungroupedRef ++ rightInfo.ungroupedRef
        )

    private[sqala] def createUnary(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        op: String,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        if op == "prior" then
            if !inConnectBy then
                report.errorAndAbort("Prior can only be used in CONNECT BY.", term.asExpr)
            else
                treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, true, inDistinctOn)
        else
            val (expr, info) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)

            val operatorExpr = op match
                case "unary_+" => '{ SqlUnaryOperator.Positive }
                case "unary_-" => '{ SqlUnaryOperator.Negative }
                case "unary_!" => '{ SqlUnaryOperator.Not }

            val sqlExpr = '{
                ($operatorExpr, $expr) match
                    case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.Like, r)) =>
                        SqlExpr.Binary(l, SqlBinaryOperator.NotLike, r)
                    case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.In, SqlExpr.Tuple(Nil))) =>
                        SqlExpr.BooleanLiteral(true)
                    case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.In, r)) =>
                        SqlExpr.Binary(l, SqlBinaryOperator.NotIn, r)
                    case (SqlUnaryOperator.Not, SqlExpr.Between(x, s, e, false)) =>
                        SqlExpr.Between(x, s, e, true)
                    case (SqlUnaryOperator.Not, SqlExpr.BooleanLiteral(false)) =>
                        SqlExpr.BooleanLiteral(true)
                    case (SqlUnaryOperator.Not, SqlExpr.BooleanLiteral(true)) =>
                        SqlExpr.BooleanLiteral(false)
                    case (o, x) =>
                        SqlExpr.Unary(x, o)
            }

            sqlExpr -> info

    private[sqala] def createBetween(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        startTerm: q.reflect.Term,
        endTerm: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        val (betweenExpr, betweenInfo) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)
        val (startExpr, startInfo) = treeInfoMacro(args, tableNames, startTerm, queryContext, inConnectBy, isPrior, inDistinctOn)
        val (endExpr, endInfo) = treeInfoMacro(args, tableNames, endTerm, queryContext, inConnectBy, isPrior, inDistinctOn)

        val sqlExpr = '{
            SqlExpr.Between($betweenExpr, $startExpr, $endExpr, false)
        }

        sqlExpr -> ExprInfo(
            hasAgg = betweenInfo.hasAgg || startInfo.hasAgg || endInfo.hasAgg,
            isAgg = false,
            isGroup = false,
            hasWindow = betweenInfo.hasWindow || startInfo.hasWindow || endInfo.hasWindow,
            isConst = false,
            columnRef = betweenInfo.columnRef ++ startInfo.columnRef ++ endInfo.columnRef,
            aggRef = betweenInfo.aggRef ++ startInfo.aggRef ++ endInfo.aggRef,
            nonAggRef = betweenInfo.nonAggRef ++ startInfo.nonAggRef ++ endInfo.nonAggRef,
            ungroupedRef = betweenInfo.ungroupedRef ++ startInfo.ungroupedRef ++ endInfo.ungroupedRef
        )

    private[sqala] def createIn(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        inTerm: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        val (expr, info) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)
        val (inExpr, inInfo) = inTerm.tpe.asType match
            case '[Seq[t]] =>
                val asSqlExpr = Expr.summon[Merge[t]]
                val expr = asSqlExpr match
                    case None => missMatch(term)
                    case Some(e) =>
                        val valueExpr = inTerm.asExprOf[Seq[t]]
                        '{ SqlExpr.Tuple($valueExpr.map(i => $e.asSqlExpr(i)).toList) }
                val info = ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
                expr -> info
            case '[Query[?, ?]] =>
                val expr = inTerm.asExprOf[Query[?, ?]]
                val sqlExpr = '{ SqlExpr.SubQuery($expr.ast) }
                sqlExpr -> ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
            case _ =>
                treeInfoMacro(args, tableNames, inTerm, queryContext, inConnectBy, isPrior, inDistinctOn)

        val sqlExpr = '{
            SqlExpr.Binary($expr, SqlBinaryOperator.In, $inExpr)
        }

        sqlExpr -> ExprInfo(
            hasAgg = info.hasAgg || inInfo.hasAgg,
            isAgg = false,
            isGroup = false,
            hasWindow = info.hasWindow || inInfo.hasWindow,
            isConst = false,
            columnRef = info.columnRef ++ inInfo.columnRef,
            aggRef = info.aggRef ++ inInfo.aggRef,
            nonAggRef = info.nonAggRef ++ inInfo.nonAggRef,
            ungroupedRef = info.ungroupedRef ++ inInfo.ungroupedRef
        )

    private[sqala] def createTuple(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        terms: List[q.reflect.Term],
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        val exprs = terms
            .map(t => treeInfoMacro(args, tableNames, t, queryContext, inConnectBy, isPrior, inDistinctOn))
        val listExpr = Expr.ofList(exprs.map(_._1))
        val info = exprs.map(_._2)
        val sqlExpr = '{
            SqlExpr.Tuple($listExpr)
        }
        sqlExpr -> ExprInfo(
            hasAgg = info.map(_.hasAgg).fold(false)(_ || _),
            isAgg = false,
            isGroup = false,
            hasWindow = info.map(_.hasAgg).fold(false)(_ || _),
            isConst = false,
            columnRef = info.map(_.columnRef).fold(Nil)(_ ++ _),
            aggRef = info.map(_.aggRef).fold(Nil)(_ ++ _),
            nonAggRef = info.map(_.nonAggRef).fold(Nil)(_ ++ _),
            ungroupedRef = info.map(_.ungroupedRef).fold(Nil)(_ ++ _)
        )

    private[sqala] def createCast(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        cast: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        cast.tpe.asType match
            case '[Cast[_, _]] =>
                val castExpr = cast.asExprOf[Cast[?, ?]]
                val (expr, info) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)
                val sqlExpr = '{
                    SqlExpr.Cast($expr, $castExpr.castType)
                }
                sqlExpr -> info.copy(isConst = false)

    private[sqala] def createInterval(using q: Quotes)(term: q.reflect.Term): (Expr[SqlExpr], ExprInfo) =
        term.tpe.asType match
            case '[TimeInterval] =>
                val expr = term.asExprOf[TimeInterval]
                val sqlExpr = '{
                   SqlExpr.Interval($expr.n, $expr.unit)
                }
                sqlExpr -> ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = Nil,
                    aggRef = Nil,
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )

    private[sqala] def createExtract(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        unit: String,
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        val unitExpr = unit match
            case "year" => '{ SqlTimeUnit.Year }
            case "month" => '{ SqlTimeUnit.Month }
            case "week" => '{ SqlTimeUnit.Week }
            case "day" => '{ SqlTimeUnit.Day }
            case "hour" => '{ SqlTimeUnit.Hour }
            case "minute" => '{ SqlTimeUnit.Minute }
            case "second" => '{ SqlTimeUnit.Second }
            case _ => missMatch(term)
        val (expr, info) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)
        val sqlExpr = '{
            SqlExpr.Extract($unitExpr, $expr)
        }
        sqlExpr -> info.copy(isConst = false)

    private[sqala] def removeNestedApply(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
        import q.reflect.*

        term match
            case Apply(a@Apply(_, _), _) => a
            case _ => term

    private[sqala] def createGrouping(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        term match
            case Apply(Apply(Ident("grouping"), Typed(Repeated(items, _), _) :: Nil), _) =>
                val info = items
                    .map(i => treeInfoMacro(args, tableNames, i, queryContext, inConnectBy, isPrior, inDistinctOn))
                for i <- info do
                    if !i._2.isGroup then
                        report.error(
                            "Arguments to GROUPING must be grouping expressions of the associated query level.",
                            term.asExpr
                        )
                val paramsExpr = Expr.ofList(info.map(_._1))
                val sqlExpr = '{
                    SqlExpr.Func("GROUPING", $paramsExpr)
                }
                sqlExpr -> ExprInfo(
                    hasAgg = true,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = info.map(_._2).flatMap(_.columnRef),
                    aggRef = info.map(_._2).flatMap(_.columnRef),
                    nonAggRef = Nil,
                    ungroupedRef = Nil
                )
            case _ => missMatch(term)

    private[sqala] def createAgg(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        name: String,
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
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

        val filterParam = allParams.find(_._1 == "filter").map(_._2)
        filterParam.foreach: p =>
            p.tpe.asType match
                case '[Boolean] =>
                case _ => report.error("The type of the parameter \"filter\" must be Boolean.", term.asExpr)

        if distinct && valueParams.isEmpty then
            report.error("Aggregates with DISTINCT must be able to sort their inputs.", term.asExpr)

        if distinct && withinGroupParam.isDefined then
            report.error(s"Cannot use DISTINCT with WITHIN GROUP.", term.asExpr)

        val valueParamInfo = valueParams
            .map(p => treeInfoMacro(args, tableNames, p, queryContext, inConnectBy, isPrior, inDistinctOn))
        val valueParamExpr = Expr.ofList(valueParamInfo.map(_._1))

        val sortByList = sortByParam.toList.map:
            case Typed(Repeated(x, _), _) => x
            case x => x :: Nil
        .flatten
        val sortByInfo = sortByList
            .map(s => sortInfoMacro(args, tableNames, s, queryContext, inConnectBy, isPrior, inDistinctOn))
        val sortByExpr = Expr.ofList(sortByInfo.map(_._1))

        val withinGroupList = withinGroupParam.toList.map:
            case Typed(Repeated(x, _), _) => x
            case x => x :: Nil
        .flatten
        val withinGroupInfo = withinGroupList
            .map(s => sortInfoMacro(args, tableNames, s, queryContext, inConnectBy, isPrior, inDistinctOn))
        val withinGroupExpr = Expr.ofList(withinGroupInfo.map(_._1))

        val filterInfo = filterParam
            .map(p => treeInfoMacro(args, tableNames, p, queryContext, inConnectBy, isPrior, inDistinctOn))
            .toList
        val filterExpr = Expr.ofList(filterInfo.map(_._1))

        val paramsInfo =
            valueParamInfo.map(_._2) ++
            sortByInfo.map(_._2) ++
            withinGroupInfo.map(_._2) ++
            filterInfo.map(_._2)

        for p <- paramsInfo do
            if p.hasAgg then
                report.error("Aggregate function calls cannot be nested.", term.asExpr)
            if p.hasWindow then
                report.error("Aggregate function calls cannot contain window function calls.", term.asExpr)
        val columnsRef =
            paramsInfo.flatMap(_.columnRef)
        if !columnsRef.map(_._1).forall(args.contains) then
            report.error("Outer query columns are not allowed in aggregate functions.", term.asExpr)

        val nameExpr = Expr(name)
        val distinctExpr = Expr(distinct)

        val sqlExpr = '{
            SqlExpr.Func(
                $nameExpr,
                $valueParamExpr,
                if $distinctExpr then Some(SqlParam.Distinct) else None,
                $sortByExpr,
                $withinGroupExpr,
                $filterExpr.headOption
            )
        }

        sqlExpr -> ExprInfo(
            hasAgg = true,
            isAgg = true,
            isGroup = false,
            hasWindow = false,
            isConst = false,
            columnRef = columnsRef,
            aggRef = columnsRef,
            nonAggRef = Nil,
            ungroupedRef = Nil
        )

    private[sqala] def createFunction(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        name: String,
        term: q.reflect.Term,
        isWindow: Boolean,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        if name == columnPseudoLevel then
            if !inConnectBy then
                report.errorAndAbort("Pseudo Column \"level\" can only be used in CONNECT BY.", term.asExpr)
            val sqlExpr = '{
                SqlExpr.Column(Some(tableCte), columnPseudoLevel)
            }

            val exprInfo = ExprInfo(
                hasAgg = false,
                isAgg = false,
                isGroup = false,
                hasWindow = false,
                isConst = false,
                columnRef = (tableCte, columnPseudoLevel) :: Nil,
                aggRef = Nil,
                nonAggRef = (tableCte, columnPseudoLevel) :: Nil,
                ungroupedRef = Nil
            )

            sqlExpr -> exprInfo
        else
            val paramTerms = removeNestedApply(term) match
                case Apply(TypeApply(_, _), p) => p
                case Apply(_, p) => p

            val symbol = term.symbol
            val params = symbol.paramSymss.flatten.filter(p => p.isTerm).filter: p =>
                p.flags match
                    case f if f.is(Flags.Given) => false
                    case _ => true

            for p <- params do
                if p.name == "sortBy" then
                    report.error(
                        s"SORT BY specified, but this expression is not an aggregate function.",
                        term.asExpr
                    )
                if p.name == "withinGroup" then
                    report.error(
                        s"WITHIN GROUP specified, but this expression is not an aggregate function.",
                        term.asExpr
                    )
                if p.name == "filter" then
                    report.error(
                        s"FILTER specified, but this expression is not an aggregate function.",
                        term.asExpr
                    )

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

            val paramsData = allParams
                .map(p => treeInfoMacro(args, tableNames, p._2, queryContext, inConnectBy, isPrior, inDistinctOn))
            val paramsInfo = paramsData.map(_._2)
            val paramsExpr = Expr.ofList(paramsData.map(_._1))

            val nameExpr = Expr(name)

            val sqlExpr = '{
                SqlExpr.Func($nameExpr, $paramsExpr)
            }

            val exprInfo = if isWindow then
                ExprInfo(
                    hasAgg = false,
                    isAgg = false,
                    isGroup = false,
                    hasWindow = false,
                    isConst = false,
                    columnRef = paramsInfo.flatMap(_.columnRef),
                    aggRef = Nil,
                    nonAggRef = paramsInfo.flatMap(_.nonAggRef),
                    ungroupedRef = paramsInfo.flatMap(_.ungroupedRef)
                )
            else
                ExprInfo(
                    hasAgg = paramsInfo.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    isGroup = false,
                    hasWindow = paramsInfo.map(_.hasWindow).fold(false)(_ || _),
                    isConst = false,
                    columnRef = paramsInfo.flatMap(_.columnRef),
                    aggRef = paramsInfo.flatMap(_.aggRef),
                    nonAggRef = paramsInfo.flatMap(_.nonAggRef),
                    ungroupedRef = paramsInfo.flatMap(_.ungroupedRef)
                )

            sqlExpr -> exprInfo

    private[sqala] def createOver(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        overValue: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        val (functionExpr, functionInfo) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)

        val isWindow = term.symbol.annotations.find:
            case Apply(Select(New(TypeIdent("sqlWindow")), _), _) => true
            case _ => false
        .isDefined

        if !functionInfo.isAgg && !isWindow then
            report.error(
                "OVER specified, but expression is not a window function nor an aggregate function.",
                term.asExpr
            )

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

        val (partitionTerm, sortTerm) = value match
            case Apply(Apply(TypeApply(Ident("partitionBy"), _), p :: Nil), _) =>
                Some(p) -> None
            case Apply(Apply(TypeApply(Ident("sortBy"), _), s :: Nil), _) =>
                None -> Some(s)
            case Apply(
                Apply(
                    TypeApply(
                        Select(
                            Apply(Apply(TypeApply(Ident("partitionBy"), _), p :: Nil), _),
                            "sortBy"
                        ),
                        _
                    ),
                    s :: Nil
                ),
                _
            ) =>
                Some(p) -> Some(s)
            case _ => None -> None

        val partition = partitionTerm match
            case Some(Apply(TypeApply(Select(Ident(t), "apply"), _), ps))
                if t.startsWith("Tuple")
            =>
                ps.map(p => treeInfoMacro(args, tableNames, p, queryContext, inConnectBy, isPrior, inDistinctOn))
            case Some(p) =>
                treeInfoMacro(args, tableNames, p, queryContext, inConnectBy, isPrior, inDistinctOn) :: Nil
            case None => Nil

        val sort = sortTerm match
            case Some(Apply(TypeApply(Select(Ident(t), "apply"), _), ss))
                if t.startsWith("Tuple")
            =>
                ss.map(s => sortInfoMacro(args, tableNames, s, queryContext, inConnectBy, isPrior, inDistinctOn))
            case Some(s) =>
                sortInfoMacro(args, tableNames, s, queryContext, inConnectBy, isPrior, inDistinctOn) :: Nil
            case None => Nil

        val partitionExpr = Expr.ofList(partition.map(_._1))
        val sortExpr = Expr.ofList(sort.map(_._1))

        val windowInfo = partition.map(_._2) ++ sort.map(_._2)

        for i <- windowInfo do
            if i.hasAgg then
                report.error("Window function calls cannot contain aggregate function calls.", term.asExpr)
            if i.hasWindow then
                report.error("Window function calls cannot be nested.", term.asExpr)
            if i.ungroupedRef.nonEmpty then
                val c = i.ungroupedRef.head
                report.error(
                    s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                    term.asExpr
                )

        val sqlExpr = '{
            SqlExpr.Window(
                $functionExpr,
                $partitionExpr,
                $sortExpr,
                $frameExpr
            )
        }

        sqlExpr -> ExprInfo(
            hasAgg = false,
            isAgg = false,
            isGroup = false,
            hasWindow = true,
            isConst = false,
            columnRef = functionInfo.columnRef ++ windowInfo.flatMap(_.columnRef),
            aggRef = Nil,
            nonAggRef = functionInfo.columnRef ++ windowInfo.flatMap(_.nonAggRef),
            ungroupedRef = functionInfo.ungroupedRef ++ windowInfo.flatMap(_.ungroupedRef)
        )

    private[sqala] def createIf(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        def collectIf(
            ifTerm: q.reflect.Term,
            exprs: List[(Expr[SqlExpr], ExprInfo)]
        ): List[(Expr[SqlExpr], ExprInfo)] =
            ifTerm match
                case If(i, t, e) =>
                    val newExprs = exprs ++ List(
                        treeInfoMacro(args, tableNames, i, queryContext, inConnectBy, isPrior, inDistinctOn),
                        treeInfoMacro(args, tableNames, t, queryContext, inConnectBy, isPrior, inDistinctOn)
                    )
                    collectIf(e, newExprs)
                case t => exprs :+ treeInfoMacro(args, tableNames, t, queryContext, inConnectBy, isPrior, inDistinctOn)

        val ifInfo = collectIf(term, Nil)

        val expr = Expr.ofList(ifInfo.map(_._1))
        val info = ifInfo.map(_._2)

        val sqlExpr = '{
            val branches = $expr.dropRight(1).grouped(2).toList.map: b =>
                SqlCase(b(0), b(1))
            SqlExpr.Case(branches, $expr.takeRight(1).head)
        }

        sqlExpr -> ExprInfo(
            hasAgg = info.map(_.hasAgg).fold(false)(_ || _),
            isAgg = false,
            isGroup = false,
            hasWindow = info.map(_.hasAgg).fold(false)(_ || _),
            isConst = false,
            columnRef = info.map(_.columnRef).fold(Nil)(_ ++ _),
            aggRef = info.map(_.aggRef).fold(Nil)(_ ++ _),
            nonAggRef = info.map(_.nonAggRef).fold(Nil)(_ ++ _),
            ungroupedRef = info.map(_.ungroupedRef).fold(Nil)(_ ++ _)
        )

    private[sqala] def createMatch(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        term match
            case Match(m, c) =>
                val (matchExpr, matchInfo) = treeInfoMacro(args, tableNames, m, queryContext, inConnectBy, isPrior, inDistinctOn)
                val caseExprs = c.map:
                    case CaseDef(Ident(_), _, Block(_, v)) =>
                        val (valueExpr, valueInfo) = treeInfoMacro(args, tableNames, v, queryContext, inConnectBy, isPrior, inDistinctOn)
                        '{ SqlCase($matchExpr, $valueExpr) } -> valueInfo
                    case CaseDef(c, _, Block(_, v)) =>
                        val (caseExpr, caseInfo) = treeInfoMacro(args, tableNames, c.asExpr.asTerm, queryContext, inConnectBy, isPrior, inDistinctOn)
                        val (valueExpr, valueInfo) = treeInfoMacro(args, tableNames, v, queryContext, inConnectBy, isPrior, inDistinctOn)
                        val info = caseInfo :: valueInfo :: Nil
                        '{ SqlCase($caseExpr, $valueExpr) } -> ExprInfo(
                            hasAgg = info.map(_.hasAgg).fold(false)(_ || _),
                            isAgg = false,
                            isGroup = false,
                            hasWindow = info.map(_.hasAgg).fold(false)(_ || _),
                            isConst = false,
                            columnRef = info.map(_.columnRef).fold(Nil)(_ ++ _),
                            aggRef = info.map(_.aggRef).fold(Nil)(_ ++ _),
                            nonAggRef = info.map(_.nonAggRef).fold(Nil)(_ ++ _),
                            ungroupedRef = info.map(_.ungroupedRef).fold(Nil)(_ ++ _)
                        )
                    case _ => missMatch(term)

                val caseExpr = Expr.ofList(caseExprs.map(_._1))
                val info = caseExprs.map(_._2)

                val sqlExpr = '{
                    val branches = $caseExpr
                        .filter(c => c.whenExpr != $matchExpr)
                    val default = $caseExpr
                        .find(c => c.whenExpr == $matchExpr)
                        .map(_.thenExpr)
                        .getOrElse(SqlExpr.Null)
                    SqlExpr.Match($matchExpr, branches, default)
                }

                sqlExpr -> ExprInfo(
                    hasAgg = info.map(_.hasAgg).fold(false)(_ || _),
                    isAgg = false,
                    isGroup = false,
                    hasWindow = info.map(_.hasAgg).fold(false)(_ || _),
                    isConst = false,
                    columnRef = info.map(_.columnRef).fold(Nil)(_ ++ _),
                    aggRef = info.map(_.aggRef).fold(Nil)(_ ++ _),
                    nonAggRef = info.map(_.nonAggRef).fold(Nil)(_ ++ _),
                    ungroupedRef = info.map(_.ungroupedRef).fold(Nil)(_ ++ _)
                )

    private[sqala] def createValue(using q: Quotes)(term: q.reflect.Term): (Expr[SqlExpr], ExprInfo) =
        val sqlExpr = term.tpe.widen.asType match
            case '[Seq[t]] =>
                val asSqlExpr = Expr.summon[Merge[t]]
                asSqlExpr match
                    case None => missMatch(term)
                    case Some(e) =>
                        val valueExpr = term.asExprOf[Seq[t]]
                        '{ SqlExpr.Array($valueExpr.map(i => $e.asSqlExpr(i)).toList) }
            case '[Unit] =>
                '{ SqlExpr.Tuple(Nil) }
            case '[t] =>
                val asSqlExpr = Expr.summon[AsSqlExpr[t]]
                asSqlExpr match
                    case None => missMatch(term)
                    case Some(e) =>
                        val valueExpr = term.asExprOf[t]
                        '{ $e.asSqlExpr($valueExpr) }

        sqlExpr -> ExprInfo(
            hasAgg = false,
            isAgg = false,
            isGroup = false,
            hasWindow = false,
            isConst = true,
            columnRef = Nil,
            aggRef = Nil,
            nonAggRef = Nil,
            ungroupedRef = Nil
        )

    private[sqala] def sortInfoMacro(using q: Quotes)(
        args: List[String],
        tableNames: Expr[List[String]],
        term: q.reflect.Term,
        queryContext: Expr[QueryContext],
        inConnectBy: Boolean,
        isPrior: Boolean,
        inDistinctOn: Boolean
    ): (Expr[SqlOrderBy], ExprInfo) =
        import q.reflect.*

        term match
            case Apply(Apply(TypeApply(Ident(op), _), v :: Nil), _)
                if List(
                    "asc",
                    "ascNullsFirst",
                    "ascNullsLast",
                    "desc",
                    "descNullsFirst",
                    "descNullsLast"
                ).contains(op)
            =>
                val (expr, info) = treeInfoMacro(args, tableNames, v, queryContext, inConnectBy, isPrior, inDistinctOn)
                val sqlExpr = op match
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
                sqlExpr -> info
            case _ =>
                val (expr, info) = treeInfoMacro(args, tableNames, term, queryContext, inConnectBy, isPrior, inDistinctOn)
                '{ SqlOrderBy($expr, Some(SqlOrderByOption.Asc), None) } -> info
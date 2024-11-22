package sqala.macros

import sqala.ast.expr.*
import sqala.ast.order.*
import sqala.dsl.*

import scala.quoted.*

object ExprMacro:
    case class ExprInfo(
        hasAgg: Boolean,
        isAgg: Boolean,
        isGroup: Boolean,
        hasWindow: Boolean,
        isConst: Boolean,
        isColumn: Boolean,
        columnRef: List[(String, String)],
        aggRef: List[(String, String)],
        nonAggRef: List[(String, String)],
        ungroupedRef: List[(String, String)]
    )

    inline def test[T](inline v: T, containers: List[(String, Container)]): SqlExpr =
        ${ testMacro[T]('v, 'containers) }

    def testMacro[T](v: Expr[T], containers: Expr[List[(String, Container)]])(using q: Quotes): Expr[SqlExpr] =
        import q.reflect.*
        val term = v.asTerm
        val (args, body) = term match
            case Inlined(_, _, Block(DefDef(_, params :: Nil, _, Some(body)) :: Nil, _)) =>
                (
                    params.params.asInstanceOf[List[ValDef]].map:
                        case ValDef(argName, argType, argTerm) =>
                            argName
                    ,
                    body
                )
            case _ => missMatch(term)
        val (sqlExpr, info) = treeInfoMacro(args, body, containers)
        sqlExpr

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

        report.error(s"${term}") // TODO 删掉
        report.errorAndAbort(s"The expression \"${term.show}\" cannot be converted to SQL expression.", term.asExpr)

    private def validateDiv(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        val isZero = term match
            case Literal(IntConstant(0)) => true
            case Literal(LongConstant(0L)) => true
            case Literal(FloatConstant(0F)) => true
            case Literal(DoubleConstant(0D)) => true
            case _ => false
        if isZero then report.error("Division by zero.", term.asExpr)

    private def validateQuery(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        term.tpe.asType match
            case '[Query[_, ManyRows]] =>
                report.error("Subquery must return only one row.", term.asExpr)
            case _ =>

    private def validateNotSubLink(using q: Quotes)(term: q.reflect.Term): Unit =
        import q.reflect.*

        term.tpe.asType match
            case '[AnySubLink[_]] =>
                report.error("ANY subquery can only appear on the right side of binary operations.", term.asExpr)
            case '[AllSubLink[_]] =>
                report.error("ALL subquery can only appear on the right side of binary operations.", term.asExpr)
            case _ =>

    private def treeInfoMacro(using q: Quotes)(
        args: List[String],
        term: q.reflect.Term,
        containers: Expr[List[(String, Container)]]
    ): (Expr[SqlExpr], ExprInfo) =
        import q.reflect.*

        term match
            // TODO some none 字面量
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
                val objectExpr = Expr(objectName)
                val valueExpr = Expr(valName)
                val sqlExpr = '{
                    val (_, container) = $containers.find(c => c._1 == $objectExpr).get
                    container.__fetchExpr__($valueExpr)
                }
                val info = ident.tpe.asType match
                    case '[Table[_]] =>
                        tableColumnInfo(objectName, valName)
                    case '[UngroupedTable[_]] =>
                        ungroupedTableColumnInfo(objectName, valName)
                    case '[SubQuery[_, _]] =>
                        subQueryColumnInfo(objectName, valName)
                    case '[UngroupedSubQuery[_, _]] =>
                        ungroupedSubQueryColumnInfo(objectName, valName)
                    case '[Sort[_, _]] => ???
                    case '[Group[_, _]] => ???
                    case _ => missMatch(term)
                sqlExpr -> info
            case Apply(Select(left, op), right :: Nil) if binaryOperators.contains(op) =>
                createBinary(args, left, op, right, containers)
            case Apply(Apply(Apply(TypeApply(Ident(op), _), left :: Nil), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, left, op, right, containers)
            case Apply(Apply(TypeApply(Apply(Ident(op), left :: Nil), _), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, left, op, right, containers)
            case Apply(Apply(Apply(Ident(op), left :: Nil), right :: Nil), _)
                if binaryOperators.contains(op)
            =>
                createBinary(args, left, op, right, containers)
            case Select(v, op) if unaryOperators.contains(op) =>
                createUnary(args, v, op, containers)
            // TODO 扩展的一元运算+ -
            // TODO 扩展方法运算符 between ...
            // TODO 元组、函数、聚合函数、窗口函数、cast、extract、interval、子查询、子连接、if、match
            case ident@Ident(_) => createValue(ident)
            case literal@Literal(_) => createValue(literal)
            case Apply(TypeApply(Select(Ident("Some"), "apply"), _), v :: Nil) =>
                treeInfoMacro(args, v, containers)
            case _ => missMatch(term)

    private def tableColumnInfo(tableName: String, columnName: String)(using Quotes): ExprInfo =
        ExprInfo(
            hasAgg = false,
            isAgg = false,
            isGroup = false,
            hasWindow = false,
            isConst = false,
            isColumn = false,
            columnRef = (tableName, columnName) :: Nil,
            aggRef = Nil,
            nonAggRef = (tableName, columnName) :: Nil,
            ungroupedRef = Nil
        )

    private def ungroupedTableColumnInfo(tableName: String, columnName: String)(using Quotes): ExprInfo =
        ExprInfo(
            hasAgg = false,
            isAgg = false,
            isGroup = false,
            hasWindow = false,
            isConst = false,
            isColumn = false,
            columnRef = (tableName, columnName) :: Nil,
            aggRef = Nil,
            nonAggRef = (tableName, columnName) :: Nil,
            ungroupedRef = (tableName, columnName) :: Nil
        )

    private def subQueryColumnInfo(tableName: String, columnName: String)(using Quotes): ExprInfo =
        ExprInfo(
            hasAgg = false,
            isAgg = false,
            isGroup = false,
            hasWindow = false,
            isConst = false,
            isColumn = false,
            columnRef = (tableName, columnName) :: Nil,
            aggRef = Nil,
            nonAggRef = (tableName, columnName) :: Nil,
            ungroupedRef = Nil
        )

    private def ungroupedSubQueryColumnInfo(tableName: String, columnName: String)(using Quotes): ExprInfo =
        ExprInfo(
            hasAgg = false,
            isAgg = false,
            isGroup = false,
            hasWindow = false,
            isConst = false,
            isColumn = false,
            columnRef = (tableName, columnName) :: Nil,
            aggRef = Nil,
            nonAggRef = (tableName, columnName) :: Nil,
            ungroupedRef = (tableName, columnName) :: Nil
        )

    private def createBinary(using q: Quotes)(
        args: List[String],
        left: q.reflect.Term,
        op: String,
        right: q.reflect.Term,
        containers: Expr[List[(String, Container)]]
    ): (Expr[SqlExpr], ExprInfo) =
        validateQuery(left)
        validateNotSubLink(left)
        validateQuery(right)

        if op == "/" || op == "%" then
            validateDiv(right)

        val (leftExpr, leftInfo) = treeInfoMacro(args, left, containers)
        val leftHasString = left.tpe.widen.asType match
            case '[String] => true
            case '[Option[String]] => true
            case _ => false

        val (rightExpr, rightInfo) = treeInfoMacro(args, right, containers)
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

        val sqlExpr =
            if op == "+" && (leftHasString || rightHasString) then
                '{ SqlExpr.Func("CONCAT", $leftExpr :: $rightExpr :: Nil) }
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
            isColumn = false,
            columnRef = leftInfo.columnRef ++ rightInfo.columnRef,
            aggRef = leftInfo.aggRef ++ rightInfo.aggRef,
            nonAggRef = leftInfo.nonAggRef ++ rightInfo.nonAggRef,
            ungroupedRef = leftInfo.ungroupedRef ++ rightInfo.ungroupedRef
        )

    private def createUnary(using q: Quotes)(
        args: List[String],
        term: q.reflect.Term,
        op: String,
        containers: Expr[List[(String, Container)]]
    ): (Expr[SqlExpr], ExprInfo) =
        validateNotSubLink(term)

        val (rightExpr, rightInfo) = treeInfoMacro(args, term, containers)

        val operatorExpr = op match
            case "unary_+" => '{ SqlUnaryOperator.Positive }
            case "unary_-" => '{ SqlUnaryOperator.Negative }
            case "unary_!" => '{ SqlUnaryOperator.Not }

        val sqlExpr =
            '{
                ($operatorExpr, $rightExpr) match
                    case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.Like, r)) =>
                        SqlExpr.Binary(l, SqlBinaryOperator.NotLike, r)
                    case (SqlUnaryOperator.Not, SqlExpr.Binary(l, SqlBinaryOperator.In, r)) =>
                        SqlExpr.Binary(l, SqlBinaryOperator.NotIn, r)
                    case (SqlUnaryOperator.Not, SqlExpr.Between(x, s, e, false)) =>
                        SqlExpr.Between(x, s, e, true)
                    case (SqlUnaryOperator.Not, SqlExpr.SubLink(q, SqlSubLinkType.Exists)) =>
                        SqlExpr.SubLink(q, SqlSubLinkType.NotExists)
                    case (o, x) =>
                        SqlExpr.Unary(x, o)
            }

        sqlExpr -> rightInfo

    private def createValue(using q: Quotes)(term: q.reflect.Term): (Expr[SqlExpr], ExprInfo) =
        val sqlExpr = term.tpe.widen.asType match
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
            isConst = false,
            isColumn = false,
            columnRef = Nil,
            aggRef = Nil,
            nonAggRef = Nil,
            ungroupedRef = Nil
        )

    private def sortInfoMacro(using q: Quotes)(
        args: List[(String, q.reflect.TypeRepr)],
        term: q.reflect.Term
    ): (ExprInfo, SqlOrderBy) =
        ???
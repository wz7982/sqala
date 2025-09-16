package sqala.static.dsl.analysis

import scala.NamedTuple.NamedTuple
import scala.quoted.{Expr, Quotes, Type}

private[sqala] object AnalysisMacro:
    inline def analysis(inline tree: Any): Unit =
        ${ AnalysisMacroImpl.analysis('tree) }

private[sqala] object AnalysisMacroImpl:
    case class ExprInfo(
        expr: Expr[?],
        hasAgg: Boolean,
        isAgg: Boolean,
        hasWindow: Boolean,
        isValue: Boolean,
        ungroupedPaths: List[List[String]],
        notInAggPaths: List[List[String]]
    )

    case class GroupInfo(
        currentTables: List[String],
        groupedExprs: List[Expr[?]]
    )

    enum ResultSize:
        case ZeroOrOne
        case Many

    case class QueryInfo(
        currentTableAlias: List[String],
        allTables: List[String],
        groupInfo: List[GroupInfo],
        sortExprs: List[Expr[?]],
        resultSize: ResultSize
    )

    def binaryOperators: List[String] =
        List(
            "==", "!=", "===", "<>", ">", ">=", "<", "<=", "&&", "||", "<=>",
            "+", "-", "*", "/", "%", "++", "overlaps", "plusInterval", "minusInterval",
            "like", "contains", "startsWith", "endsWith"
        )

    def unaryOperators: List[String] =
        List("unary_+", "unary_-", "unary_!")

    def splitFunc(using q: Quotes)(term: q.reflect.Term): (List[String], q.reflect.Term) =
        import q.reflect.*

        removeEmptyInline(term) match
            case Block(DefDef(_, _, _, Some(Block(args, body))) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)
            case Block(DefDef(_, args :: Nil, _, Some(body)) :: Nil, _) =>
                val valDefList = args.asInstanceOf[List[ValDef]]
                (valDefList.map(_.name), body)

    def removeEmptyInline(using q: Quotes)(term: q.reflect.Term): q.reflect.Term =
        import q.reflect.*

        term match
            case Inlined(_, Nil, t) =>
                removeEmptyInline(t)
            case Typed(t, _) => t
            case _ => term

    def analysis(tree: Expr[Any])(using q: Quotes): Expr[Unit] = 
        import q.reflect.*

        val term = 
            removeEmptyInline(tree.asTerm) match
                case Block(_, t) =>
                    removeEmptyInline(t)

        report.errorAndAbort(s"$term")

        '{()}

    def createColumnInfo(using q: Quotes)(
        term: q.reflect.Term,
        objectName: String,
        valName: String,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val path = objectName :: valName :: Nil
        val inGroup = groupInfo
            .find(g => g.currentTables.contains(objectName))
            .map(g => g.groupedExprs.size > 0)
            .getOrElse(false)
        val ungroupedPaths = 
            if inGroup then path :: Nil
            else Nil
        ExprInfo(
            expr = term.asExpr,
            hasAgg = false,
            isAgg = false,
            hasWindow = false,
            isValue = false,
            ungroupedPaths = ungroupedPaths,
            notInAggPaths = path :: Nil
        )

    def createTupleExpr(using q: Quotes)(
        terms: List[q.reflect.Term],
        allTables: List[(argName: String, tableName: String)],
        inConnectBy: Boolean,
        groupInfo: List[GroupInfo]
    ): ExprInfo =
        val infoList = terms.map(t => createExpr(t, allTables, inConnectBy, groupInfo))
        ExprInfo(
            expr = SqlExpr.Tuple(infoList.map(_.expr)),
            hasAgg = infoList.map(_.hasAgg).fold(false)(_ || _),
            isAgg = false,
            hasWindow = infoList.map(_.hasWindow).fold(false)(_ || _),
            isValue = false,
            ungroupedPaths = infoList.flatMap(_.ungroupedPaths),
            notInAggPaths = infoList.flatMap(_.notInAggPaths)
        )
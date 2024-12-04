package sqala.macros

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.dsl.*

import scala.quoted.* 

object ClauseMacro:
    inline def fetchArgNames[T](inline f: T): List[String] =
        ${ fetchArgNamesMacro[T]('f) }

    inline def analysisFilter[T](inline f: T): QueryContext => SqlExpr =
        ${ analysisFilterMacro[T]('f) }

    transparent inline def analysisSelect[T, N <: Tuple, V <: Tuple](
        inline f: T, 
        ast: SqlQuery.Select,
        qc: QueryContext
    ): ProjectionQuery[N, V, ?] =
        ${ analysisSelectMacro[T, N, V]('f, 'ast, 'qc) }

    private def fetchArgNamesMacro[T](f: Expr[T])(using Quotes): Expr[List[String]] =
        val (args, _) = unwrapFuncMacro(f)

        Expr(args)

    private def analysisFilterMacro[T](
        f: Expr[T]
    )(using q: Quotes): Expr[QueryContext => SqlExpr] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)
        val (expr, info) = ExprMacro.treeInfoMacro(args, body)

        if info.hasAgg then
            report.error("Aggregate functions are not allowed in WHERE/ON.", body.asExpr)

        if info.hasWindow then
            report.error("Window functions are not allowed in WHERE/ON.", body.asExpr)

        expr

    private def analysisSelectMacro[T, N <: Tuple : Type, V <: Tuple : Type](
        f: Expr[T], 
        ast: Expr[SqlQuery.Select],
        qc: Expr[QueryContext]
    )(using q: Quotes): Expr[ProjectionQuery[N, V, ?]] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)

        val terms = body.tpe.asType match
            case '[type t <: Tuple; t] =>
                body match
                    case Apply(_, applyTerms) => applyTerms
            case '[type t <: scala.NamedTuple.AnyNamedTuple; t] =>
                body match
                    case Apply(_, Apply(_, applyTerms) :: Nil) =>
                        applyTerms
                    case _ => Nil
            case _ => Nil

        val exprInfo = terms
            .map(t => ExprMacro.treeInfoMacro(args, t, false))

        val info = exprInfo.map(_._2)

        val hasAgg = info.map(_.hasAgg).fold(false)(_ || _)
            if hasAgg then
                for i <- info if i.nonAggRef.nonEmpty do
                    val c = i.nonAggRef.head
                    report.error(
                        s"Column \"${c._1}.${c._2}\" must appear in the GROUP BY clause or be used in an aggregate function.",
                        body.asExpr
                    )
                
        val isAgg = info.map(_.hasAgg).forall(i => i)

        val selectExpr = Expr.ofList(exprInfo.map(_._1))

        val newAstExpr = '{ (context: QueryContext) =>
            val selectItems = $selectExpr.zipWithIndex.map: (e, i) =>
                SqlSelectItem.Item(e(context), Some(s"c$i"))
            $ast.copy(select = selectItems)
        }

        if isAgg then
            '{
                new ProjectionQuery[N, V, OneRow]($newAstExpr($qc))(using $qc)
            }
        else 
            '{
                new ProjectionQuery[N, V, ManyRows]($newAstExpr($qc))(using $qc)
            }

    private def unwrapFuncMacro[T](
        value: Expr[T]
    )(using q: Quotes): (args: List[String], body: q.reflect.Term) =
        import q.reflect.*

        def unwrapInlined(term: Term): Term =
            term match
                case Inlined(_, _, inlinedTerm) =>
                    unwrapInlined(inlinedTerm)
                case _ => term

        val term = value.asTerm

        val func = unwrapInlined(term) match
            case Block(_, Inlined(_, _, Block(statement :: Nil, _))) =>
                statement
            case Block(_, blockTerm) =>
                unwrapInlined(blockTerm) match
                    case Block(statement :: Nil, _) =>
                        statement
            case _ => report.errorAndAbort("Unsupported usage.")

        val args = func match
            case DefDef(_, params :: Nil, _, _) =>
                params.params.asInstanceOf[List[ValDef]].map:
                    case ValDef(argName, _, _) =>
                        argName
            case _ => report.errorAndAbort("Unsupported usage.")

        val body = func match
            case DefDef(_, _, _, Some(funBody)) =>
                funBody
            case _ => report.errorAndAbort("Unsupported usage.")

        (args, body)
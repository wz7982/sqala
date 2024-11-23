package sqala.macros

import sqala.ast.expr.SqlExpr
import sqala.dsl.*

import scala.quoted.* 

object ClauseMacro:
    inline def fetchArgNames[T](inline f: T): List[String] =
        ${ fetchArgNamesMacro[T]('f) }

    inline def analysisFilter[T](inline f: T, containers: List[(String, Container)]): SqlExpr =
        ${ analysisFilterMacro[T]('f, 'containers) }

    private def fetchArgNamesMacro[T](f: Expr[T])(using Quotes): Expr[List[String]] =
        val (args, _) = unwrapFuncMacro(f)

        Expr(args)

    private def analysisFilterMacro[T](
        f: Expr[T], 
        containers: Expr[List[(String, Container)]]
    )(using q: Quotes): Expr[SqlExpr] =
        import q.reflect.*

        val (args, body) = unwrapFuncMacro(f)
        val (expr, info) = ExprMacro.treeInfoMacro(args, containers, body)

        if info.hasAgg then
            report.error("Aggregate functions are not allowed in WHERE/ON.", body.asExpr)

        if info.hasWindow then
            report.error("Window functions are not allowed in WHERE/ON.", body.asExpr)

        expr

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
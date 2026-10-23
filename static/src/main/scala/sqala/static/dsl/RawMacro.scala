package sqala.static.dsl

import sqala.ast.expr.SqlExpr

import scala.quoted.{Expr, Quotes, Type}
import sqala.ir.IrExpr

/**
 * Internal macro that summons `AsExpr` instances for `rawExpr`
 * interpolation arguments.
 */
object RawMacro:
    /**
      * Converts a sequence of values to a list of `SqlExpr` using the
      */
    inline def asSqlExprs[CL <: Int](inline expr: Seq[Any]): List[IrExpr] =
        ${ RawMacroImpl.asIrExprs('expr) }

object RawMacroImpl:
    def asIrExprs[CL <: Int : Type](expr: Expr[Seq[Any]])(using q: Quotes): Expr[List[IrExpr]] =
        import q.reflect.*

        def removeInlined(term: Term): Term =
            term match
                case Inlined(None, Nil, t) => removeInlined(t)
                case _ => term

        val term = removeInlined(expr.asTerm)

        val terms = term match
            case Typed(Repeated(terms, _), _) => terms

        val sqlExprs =
            for term <- terms yield
                val tpe = term.tpe.widen.asType
                tpe match
                    case '[t] =>
                        val instance = Expr.summon[AsExpr[t, CL]].get
                        '{ $instance.asErasedExpr(${ term.asExprOf[t] }).asIrExpr }

        Expr.ofList(sqlExprs)
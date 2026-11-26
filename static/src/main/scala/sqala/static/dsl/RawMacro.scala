package sqala.static.dsl

import sqala.ir.IrExpr

import scala.quoted.{Expr, Quotes, Type}

/**
 * Internal macro that summons `AsExpr` instances for `rawExpr`
 * interpolation arguments.
 */
object RawMacro:
    /**
      * Converts a sequence of values to a list of `SqlExpr` using the
      */
    inline def asIrExprs[CL <: Int](inline expr: Seq[Any])(using qc: QueryContext[CL]): List[IrExpr] =
        ${ RawMacroImpl.asIrExprs('expr, 'qc) }

object RawMacroImpl:
    def asIrExprs[CL <: Int : Type](expr: Expr[Seq[Any]], qc: Expr[QueryContext[CL]])(using q: Quotes): Expr[List[IrExpr]] =
        import q.reflect.*

        // Referencing the evidence pins CL to its singleton level; without it
        // Type[CL] arrives widened to Int and every scope check fails.
        val _ = qc

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
                        val instanceOption = Expr.summon[AsExpr[t, CL]]
                        val instance =
                            if instanceOption.isDefined then
                                instanceOption.get
                            else
                                report.errorAndAbort(s"Cannot use ${Type.show[t]} as a rawExpr argument.")
                        '{ $instance.asErasedExpr(${ term.asExprOf[t] }).expr }

        Expr.ofList(sqlExprs)
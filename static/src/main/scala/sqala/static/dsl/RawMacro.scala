package sqala.static.dsl

import sqala.ast.expr.SqlExpr

import scala.quoted.{Expr, Quotes, Type}

/**
 * Internal macro that summons `AsExpr` instances for `rawExpr`
 * interpolation arguments.
 */
private[sqala] object RawMacro:
    /**
      * Converts a sequence of values to a list of `SqlExpr` using the
      */
    inline def asSqlExprs[CL <: Int](inline expr: Seq[Any]): List[SqlExpr] =
        ${ RawMacroImpl.asSqlExprs('expr) }

private[sqala] object RawMacroImpl:
    def asSqlExprs[CL <: Int : Type](expr: Expr[Seq[Any]])(using q: Quotes): Expr[List[SqlExpr]] =
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
                        '{ $instance.asErasedExpr(${ term.asExprOf[t] }).asSqlExpr }

        Expr.ofList(sqlExprs)
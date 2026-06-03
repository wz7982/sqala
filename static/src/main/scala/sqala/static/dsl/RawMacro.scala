package sqala.static.dsl

import scala.quoted.{Expr, Quotes, Type}

private[sqala] object RawMacro:
    inline def asExprInstances[CL <: Int](inline expr: Seq[Any]): List[AsExpr[?, ?]] =
        ${ RawMacroImpl.asExprInstances('expr) }

private[sqala] object RawMacroImpl:
    def asExprInstances[CL <: Int : Type](expr: Expr[Seq[Any]])(using q: Quotes): Expr[List[AsExpr[?, ?]]] =
        import q.reflect.*

        def removeInlined(term: Term): Term =
            term match
                case Inlined(None, Nil, t) => removeInlined(t)
                case _ => term

        val term = removeInlined(expr.asTerm)

        val terms = term match
            case Typed(Repeated(terms, _), _) => terms

        val instances =
            for term <- terms yield
                val tpe = term.tpe.widen.asType
                tpe match
                    case '[t] =>
                        Expr.summon[AsExpr[t, CL]].get

        Expr.ofList(instances)
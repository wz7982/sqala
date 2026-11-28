package sqala.jdbc

import scala.quoted.*

private[jdbc] object NativeSqlMacro:
    def build(sc: Expr[StringContext], args: Expr[Seq[Any]])(using Quotes): Expr[NativeSql] =
        import quotes.reflect.*

        val argExprs: List[Expr[Any]] =
            args match
                case Varargs(exprs) =>
                    exprs.toList
                case _ =>
                    report.errorAndAbort("The sql interpolator requires explicit arguments.", args)

        val values: List[Expr[Either[JdbcArg[?], Seq[JdbcArg[?]]]]] =
            argExprs.map: arg =>
                val tpe = arg.asTerm.tpe.widen
                tpe.asType match
                    case '[Seq[t]] =>
                        val seqExpr = arg.asExprOf[Seq[t]]
                        val encoder = Expr.summon[JdbcEncoder[t]].getOrElse:
                            report.errorAndAbort(
                                s"No JdbcEncoder[${Type.show[t]}] found for the SQL parameter. Define a given JdbcEncoder[${Type.show[t]}] to bind this type.",
                                arg.asTerm.pos
                            )
                        '{
                            val value: Either[JdbcArg[?], Seq[JdbcArg[?]]] =
                                Right($seqExpr.map: item =>
                                    JdbcArg[t](item)(using $encoder)
                                )
                            value
                        }
                    case '[t] =>
                        val valueExpr = arg.asExprOf[t]
                        val encoder = Expr.summon[JdbcEncoder[t]].getOrElse:
                            report.errorAndAbort(
                                s"No JdbcEncoder[${Type.show[t]}] found for the SQL parameter. Define a given JdbcEncoder[${Type.show[t]}] to bind this type.",
                                arg.asTerm.pos
                            )
                        '{
                            val value: Either[JdbcArg[?], Seq[JdbcArg[?]]] =
                                Left(JdbcArg[t]($valueExpr)(using $encoder))
                            value
                        }

        val valuesExpr = Expr.ofList(values)

        '{ NativeSql.build($sc, $valuesExpr) }
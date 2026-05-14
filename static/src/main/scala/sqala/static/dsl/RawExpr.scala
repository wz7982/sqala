package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.metadata.AsSqlExpr

final case class RawExpr[L <: Int](private val words: List[String], private val exprs: List[SqlExpr]):
    def as[T: AsSqlExpr as a](using QueryContext[L]): Expr[Wrap[T, Option], Value] =
        Expr(SqlExpr.Custom(words, exprs))

object RawExpr:
    def apply[L <: Int](words: List[String], instances: List[AsExpr[?, ?]], args: List[Any]): RawExpr[L] =
        val sqlExprs = instances.zip(args).map((i, e) => i.asInstanceOf[AsExpr[Any, ?]].asExpr(e).asSqlExpr)
        new RawExpr(words, sqlExprs)
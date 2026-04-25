package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.ast.quantifier.SqlQuantifier
import sqala.static.metadata.AsSqlExpr

class RawExpr(private val words: List[String], private val exprs: List[SqlExpr]):
    def as[T: AsSqlExpr as a](using QueryContext): Expr[Wrap[T, Option], Value] =
        Expr(SqlExpr.Custom(words, exprs))

object RawExpr:
    def apply(words: List[String], instances: List[AsExpr[?]], args: List[Any]): RawExpr =
        val sqlExprs = instances.zip(args).map((i, e) => i.asInstanceOf[AsExpr[Any]].asExpr(e).asSqlExpr)
        new RawExpr(words, sqlExprs)

class RawQuantifier(private val words: List[String], private val exprs: List[SqlExpr]):
    private[sqala] def asSqlQuantifier: SqlQuantifier =
        SqlQuantifier.Custom(words, exprs)

object RawQuantifier:
    def apply(words: List[String], instances: List[AsExpr[?]], args: List[Any]): RawQuantifier =
        val sqlExprs = instances.zip(args).map((i, e) => i.asInstanceOf[AsExpr[Any]].asExpr(e).asSqlExpr)
        new RawQuantifier(words, sqlExprs)
package sqala.metadata

import sqala.ast.expr.SqlExpr
import sqala.common.AsSqlExpr

trait CustomField[T, R](using a: AsSqlExpr[R]) extends AsSqlExpr[T]:
    def toValue(x: T): R

    def fromValue(x: R): T
        
    override def asSqlExpr(x: T): SqlExpr = a.asSqlExpr(toValue(x))
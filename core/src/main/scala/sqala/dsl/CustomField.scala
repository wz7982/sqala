package sqala.dsl

import sqala.ast.expr.SqlExpr

trait CustomField[T, R](using a: AsSqlExpr[R]) extends AsSqlExpr[T]:
    def toValue(x: T): R

    def fromValue(x: R): T
        
    override def asSqlExpr(x: T): SqlExpr = a.asSqlExpr(toValue(x))
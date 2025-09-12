package sqala.static.metadata

import sqala.ast.expr.{SqlExpr, SqlType}

trait CustomField[T, R](using a: AsSqlExpr[R]) extends AsSqlExpr[T]:
    def toValue(x: T): R

    def fromValue(x: R): T
        
    def asSqlExpr(x: T): SqlExpr = a.asSqlExpr(toValue(x))

    def sqlType: SqlType = a.sqlType
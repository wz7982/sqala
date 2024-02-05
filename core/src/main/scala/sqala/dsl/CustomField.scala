package sqala.dsl

import sqala.ast.expr.SqlExpr
import sqala.jdbc.Decoder

import java.sql.ResultSet

trait CustomField[T, R](using a: AsSqlExpr[R], d: Decoder[R]) extends AsSqlExpr[T], Decoder[T]:
    def toValue(x: T): R

    def fromValue(x: R): T
        
    override def asSqlExpr(x: T): SqlExpr = a.asSqlExpr(toValue(x))

    override def decode(data: ResultSet, cursor: Int): T = fromValue(d.decode(data, cursor))

    override def offset: Int = d.offset
package sqala.printer

import sqala.ast.expr.{SqlCastType, SqlExpr}
import sqala.ast.statement.SqlStatement

class PostgresqlPrinter(override val prepare: Boolean, override val indent: Int) extends SqlPrinter(prepare):
    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" DO UPDATE SET ")

        printList(upsert.updateList): u =>
            printExpr(u)
            sqlBuilder.append(" = ")
            sqlBuilder.append("EXCLUDED.")
            printExpr(u)

    override def printCastType(castType: SqlCastType): Unit =
        val t = castType match
            case SqlCastType.Varchar => "VARCHAR"
            case SqlCastType.Int4 => "INT4"
            case SqlCastType.Int8 => "INT8"
            case SqlCastType.Float4 => "FLOAT4"
            case SqlCastType.Float8 => "FLOAT8"
            case SqlCastType.DateTime => "TIMESTAMP"
            case SqlCastType.Json => "JSONB"
            case SqlCastType.Custom(c) => c
        sqlBuilder.append(t)

    override def printIntervalExpr(expr: SqlExpr.Interval): Unit =
        sqlBuilder.append("INTERVAL '")
        sqlBuilder.append(expr.value)
        sqlBuilder.append(" ")
        sqlBuilder.append(expr.unit.unit)
        sqlBuilder.append("'")
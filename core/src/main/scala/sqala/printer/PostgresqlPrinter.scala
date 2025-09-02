package sqala.printer

import sqala.ast.expr.{SqlCastType, SqlExpr}
import sqala.ast.limit.*
import sqala.ast.statement.SqlStatement

class PostgresqlPrinter(override val enableJdbcPrepare: Boolean) extends SqlPrinter(enableJdbcPrepare):
    override def printLimit(limit: SqlLimit): Unit =
        val standardMode = limit.fetch match
            case None | Some(_, SqlFetchUnit.RowCount, SqlFetchMode.Only) =>
                false
            case _ =>
                true
        if standardMode then super.printLimit(limit)
        else
            for f <- limit.fetch do
                sqlBuilder.append("LIMIT ")
                printExpr(f.limit)
            for o <- limit.offset do
                if limit.fetch.isDefined then
                    sqlBuilder.append(" ")
                sqlBuilder.append("OFFSET ")
                printExpr(o)

    override def printUpsert(upsert: SqlStatement.Upsert): Unit =
        sqlBuilder.append("INSERT INTO ")
        printTable(upsert.table)

        sqlBuilder.append(" (")
        printList(upsert.columns)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" VALUES (")
        printList(upsert.values)(printExpr)
        sqlBuilder.append(")")

        sqlBuilder.append(" ON CONFLICT (")
        printList(upsert.pkList)(printExpr)
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

    override def printVectorExpr(expr: SqlExpr.Vector): Unit =
        super.printVectorExpr(expr)
        sqlBuilder.append(" :: VECTOR")
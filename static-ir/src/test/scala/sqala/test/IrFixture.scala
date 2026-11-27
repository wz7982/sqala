package sqala.test

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.{SqlQuery, SqlStatement}
import sqala.dialect.PostgresqlDialect
import sqala.ir.*
import sqala.util.{NonEmptyList, queryToString, statementToString}

object IrFixture:
    def lines(xs: String*): String =
        xs.mkString("\n")

    def n(v: Int): IrExpr =
        IrExpr.Native(SqlExpr.NumberLiteral(v))

    def s(v: String): IrExpr =
        IrExpr.Native(SqlExpr.StringLiteral(v))

    def b(v: Boolean): IrExpr =
        IrExpr.Native(SqlExpr.BooleanLiteral(v))

    def col(name: String): IrExpr =
        IrExpr.Column(None, name)

    def col(t: IrTable, name: String): IrExpr =
        IrExpr.Column(Some(t), name)

    def sel(
        items: List[IrSelectItem],
        from: Option[IrTable] = None,
        where: Option[IrExpr] = None
    ): IrQuery.Select =
        IrQuery.Select(None, NonEmptyList(items.head, items.tail), from, where, None, None, Nil, None, None)

    def toAst(e: IrExpr): SqlExpr =
        new ToAst().toAst(e)

    def toAst(q: IrQuery): SqlQuery =
        new ToAst().toAst(q)

    def toAst(st: IrStatement): SqlStatement =
        new ToAst().toAst(st)

    def pg(q: IrQuery): String =
        try queryToString(toAst(q), PostgresqlDialect, true)
        catch case e: Throwable => s"!!EXCEPTION ${e.getClass.getName}: ${e.getMessage}"

    def pgExpr(e: IrExpr): String =
        pg(sel(List(IrSelectItem(e, "x"))))

    def pgStmt(st: IrStatement): String =
        try statementToString(toAst(st), PostgresqlDialect, true)
        catch case e: Throwable => s"!!EXCEPTION ${e.getClass.getName}: ${e.getMessage}"

    def runProbes(cases: List[(String, String)]): Unit =
        cases.foreach:
            case (label, out) =>
                println(s"$label|<<<$out>>>")

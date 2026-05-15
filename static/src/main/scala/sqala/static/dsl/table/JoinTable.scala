package sqala.static.dsl.table

import sqala.ast.table.{SqlJoinCondition, SqlTable}
import sqala.metadata.SqlBoolean
import sqala.static.dsl.*

final case class JoinTable[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
) extends AnyTable

final case class JoinPart[T, OKS <: Tuple, L <: Int](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
):
    def on[F](f: T => F)(using
        qc: QueryContext[L],
        a: AsExpr[F, L],
        b: SqlBoolean[a.R],
        kt: KindToTuple[a.K],
        i: CanInFilter[kt.R],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        c: CombineKindTuple[OKS, e.R]
    ): JoinTable[T, c.R, L] =
        val cond = a.asExpr(f(params))
        JoinTable(
            params,
            sqlTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        )
package sqala.static.dsl

import sqala.ast.table.{SqlTable, SqlTableAlias}
import sqala.static.metadata.FetchCompanion
import sqala.ast.table.SqlJoinType
import scala.util.NotGiven

trait AsFrom[T]:
    type R

    def table(x: T)(using QueryContext): (R, SqlTable)

// TODO 子查询和json表
object AsFrom:
    type Aux[T, O] = AsFrom[T]:
        type R = O

    given entity[O](using 
        fc: FetchCompanion[O],
        nt: NotGiven[O <:< Tuple]
    ): Aux[O, Table[fc.R]] =
        new AsFrom[O]:
            type R = Table[fc.R]

            def table(x: O)(using c: QueryContext): (R, SqlTable) =
                val metaData = fc.metaData
                c.tableIndex += 1
                val aliasName = s"t${c.tableIndex}"
                val table = Table[fc.R](
                    aliasName,
                    metaData,
                    SqlTable.Standard(
                        metaData.tableName, 
                        Some(SqlTableAlias(aliasName, Nil)),
                        None,
                        None
                    )
                )
                (table, table.__sqlTable__)

    given table[T]: Aux[Table[T], Table[T]] =
        new AsFrom[Table[T]]:
            type R = Table[T]

            def table(x: Table[T])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given funcTable[T]: Aux[FuncTable[T], FuncTable[T]] =
        new AsFrom[FuncTable[T]]:
            type R = FuncTable[T]

            def table(x: FuncTable[T])(using QueryContext): (R, SqlTable) =
                (x, x.__sqlTable__)

    given join[T]: Aux[JoinTable[T], T] =
        new AsFrom[JoinTable[T]]:
            type R = T

            def table(x: JoinTable[T])(using QueryContext): (R, SqlTable) =
                (x.params, x.sqlTable)

    given tuple[H: AsFrom as ah, T <: Tuple : AsFrom as at](using
        n: NotGiven[T =:= EmptyTuple],
        t: ToTuple[at.R]
    ): Aux[H *: T, ah.R *: t.R] =
        new AsFrom[H *: T]:
            type R = ah.R *: t.R

            def table(x: H *: T)(using QueryContext): (R, SqlTable) =
                val (headParam, headTable) = ah.table(x.head)
                val (tailParam, tailTable) = at.table(x.tail)
                val param = headParam *: t.toTuple(tailParam)
                val table = SqlTable.Join(headTable, SqlJoinType.Cross, tailTable, None)
                (param, table)

    given tuple1[H: AsFrom as ah]: Aux[H *: EmptyTuple, ah.R] =
        new AsFrom[H *: EmptyTuple]:
            type R = ah.R

            def table(x: H *: EmptyTuple)(using QueryContext): (R, SqlTable) =
                ah.table(x.head)
package sqala.dsl.statement.query

import sqala.dsl.*

import scala.NamedTuple.*
import scala.compiletime.constValue

class UngroupedTable[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val columnMap = __metaData__.fieldNames.zip(__metaData__.columnNames).toMap
        Expr.Column(__aliasName__, columnMap(name))

class UngroupedSubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __alias__ : String,
    private[sqala] val __columnSize__ : Int
)(using val qc: QueryContext) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Expr[?] =
        val index = constValue[Index[N, name.type, 0]]
        Expr.Column(__alias__, s"c${index}")

trait ToUngrouped[T]:
    type R

    def toUngrouped(x: T): R

object ToUngrouped:
    type Aux[T, O] = ToUngrouped[T]:
        type R = O

    given tableToUngrouped[T]: Aux[Table[T], UngroupedTable[T]] = new ToUngrouped[Table[T]]:
        type R = UngroupedTable[T]

        def toUngrouped(x: Table[T]): R =
            new UngroupedTable(x.__tableName__, x.__aliasName__, x.__metaData__)

    given subQueryToUngrouped[N <: Tuple, V <: Tuple]: Aux[SubQuery[N, V], UngroupedSubQuery[N, V]] =
        new ToUngrouped[SubQuery[N, V]]:
            type R = UngroupedSubQuery[N, V]

            def toUngrouped(x: SubQuery[N, V]): R =
                new UngroupedSubQuery(x.__alias__, x.__columnSize__)(using x.qc)

    given tupleToUngrouped[H, T <: Tuple](using 
        h: ToUngrouped[H],
        t: ToUngrouped[T],
        to: ToTuple[t.R]
    ): Aux[H *: T, h.R *: to.R] =
        new ToUngrouped[H *: T]:
            type R = h.R *: to.R

            def toUngrouped(x: H *: T): R =
                h.toUngrouped(x.head) *: to.toTuple(t.toUngrouped(x.tail))

    given emptyTupleToUngrouped: Aux[EmptyTuple, EmptyTuple] =
        new ToUngrouped[EmptyTuple]:
            type R = EmptyTuple

            def toUngrouped(x: EmptyTuple): R = x
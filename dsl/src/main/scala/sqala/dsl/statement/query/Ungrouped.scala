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

trait ToUngroup[T]:
    type R

    def toUngroup(x: T): R

object ToUngroup:
    type Aux[T, O] = ToUngroup[T]:
        type R = O

    given tableToUngroup[T]: Aux[Table[T], UngroupedTable[T]] = new ToUngroup[Table[T]]:
        type R = UngroupedTable[T]

        def toUngroup(x: Table[T]): R =
            new UngroupedTable(x.__tableName__, x.__aliasName__, x.__metaData__)

    given subQueryToUngroup[N <: Tuple, V <: Tuple]: Aux[SubQuery[N, V], UngroupedSubQuery[N, V]] =
        new ToUngroup[SubQuery[N, V]]:
            type R = UngroupedSubQuery[N, V]

            def toUngroup(x: SubQuery[N, V]): R =
                new UngroupedSubQuery(x.__alias__, x.__columnSize__)(using x.qc)

    given tupleToUngroup[H, T <: Tuple](using 
        h: ToUngroup[H],
        t: ToUngroup[T],
        to: ToTuple[t.R]
    ): Aux[H *: T, h.R *: to.R] =
        new ToUngroup[H *: T]:
            type R = h.R *: to.R

            def toUngroup(x: H *: T): R =
                h.toUngroup(x.head) *: to.toTuple(t.toUngroup(x.tail))

    given emptyTupleToUngroup: Aux[EmptyTuple, EmptyTuple] =
        new ToUngroup[EmptyTuple]:
            type R = EmptyTuple

            def toUngroup(x: EmptyTuple): R = x
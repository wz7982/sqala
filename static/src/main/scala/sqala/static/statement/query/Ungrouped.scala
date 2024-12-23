package sqala.static.statement.query

import sqala.static.common.*

import scala.NamedTuple.*

class UngroupedTable[T](
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Any = compileTimeOnly

class UngroupedSubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __columns__ : List[String]
) extends Selectable:
    type Fields = NamedTuple[N, V]

    def selectDynamic(name: String): Any = compileTimeOnly

trait ToUngrouped[T]:
    type R

    def toUngrouped(x: T): R

object ToUngrouped:
    type Aux[T, O] = ToUngrouped[T]:
        type R = O

    given table[T]: Aux[Table[T], UngroupedTable[T]] = new ToUngrouped[Table[T]]:
        type R = UngroupedTable[T]

        def toUngrouped(x: Table[T]): R =
            UngroupedTable(x.__metaData__)

    given subQuery[N <: Tuple, V <: Tuple]: Aux[SubQuery[N, V], UngroupedSubQuery[N, V]] =
        new ToUngrouped[SubQuery[N, V]]:
            type R = UngroupedSubQuery[N, V]

            def toUngrouped(x: SubQuery[N, V]): R =
                new UngroupedSubQuery(x.__columns__)

    given tuple[H, T <: Tuple](using
        h: ToUngrouped[H],
        t: ToUngrouped[T],
        to: ToTuple[t.R]
    ): Aux[H *: T, h.R *: to.R] =
        new ToUngrouped[H *: T]:
            type R = h.R *: to.R

            def toUngrouped(x: H *: T): R =
                h.toUngrouped(x.head) *: to.toTuple(t.toUngrouped(x.tail))

    given emptyTuple: Aux[EmptyTuple, EmptyTuple] =
        new ToUngrouped[EmptyTuple]:
            type R = EmptyTuple

            def toUngrouped(x: EmptyTuple): R = x
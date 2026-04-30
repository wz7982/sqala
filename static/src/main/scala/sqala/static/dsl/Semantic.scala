package sqala.static.dsl

import scala.NamedTuple.NamedTuple
import scala.util.NotGiven

type ExprKind

type Column <: ExprKind

type Operation <: ExprKind

type Agg <: ExprKind

type AggOperation <: ExprKind

type WindowWithoutOver <: ExprKind

type Window <: ExprKind

type WindowEmpty <: ExprKind

type WindowAgg <: ExprKind

type WindowGrouped <: ExprKind

type Grouped <: ExprKind

type GroupedOperation <: ExprKind

type Ungrouped <: ExprKind

type Value <: ExprKind

type ValueOperation <: ExprKind

type Distinct <: ExprKind

type QuerySize

type OneRow <: QuerySize

type ManyRows <: QuerySize

trait KindOperation[A <: ExprKind, B <: ExprKind]:
    type R <: ExprKind

object KindOperation:
    type Aux[A <: ExprKind, B <: ExprKind, O <: ExprKind] =
        KindOperation[A, B]:
            type R = O

    given swap[A <: ExprKind, B <: ExprKind](using
        o: KindOperation[A, B],
        n: NotGiven[A =:= B]
    ): Aux[B, A, o.R] =
        new KindOperation[B, A]:
            type R = o.R

    given columnAndColumn: Aux[Column, Column, Operation] =
        new KindOperation[Column, Column]:
            type R = Operation

    given columnAndOperation: Aux[Column, Operation, Operation] =
        new KindOperation[Column, Operation]:
            type R = Operation

    given columnAndWindow: Aux[Column, Window, Window] =
        new KindOperation[Column, Window]:
            type R = Window

    given columnAndWindowEmpty: Aux[Column, WindowEmpty, Window] =
        new KindOperation[Column, WindowEmpty]:
            type R = Window

    given columnAndWindowGrouped: Aux[Column, WindowGrouped, Window] =
        new KindOperation[Column, WindowGrouped]:
            type R = Window

    given columnAndGrouped: Aux[Column, Grouped, Operation] =
        new KindOperation[Column, Grouped]:
            type R = Operation

    given columnAndGroupedOperation: Aux[Column, GroupedOperation, Operation] =
        new KindOperation[Column, GroupedOperation]:
            type R = Operation

    given columnAndValue: Aux[Column, Value, Operation] =
        new KindOperation[Column, Value]:
            type R = Operation

    given columnAndValueOperation: Aux[Column, ValueOperation, Operation] =
        new KindOperation[Column, ValueOperation]:
            type R = Operation

    given operationAndOperation: Aux[Operation, Operation, Operation] =
        new KindOperation[Operation, Operation]:
            type R = Operation

    given operationAndWindow: Aux[Operation, Window, Window] =
        new KindOperation[Operation, Window]:
            type R = Window

    given operationAndWindowEmpty: Aux[Operation, WindowEmpty, Window] =
        new KindOperation[Operation, WindowEmpty]:
            type R = Window

    given operationAndWindowGrouped: Aux[Operation, WindowGrouped, Window] =
        new KindOperation[Operation, WindowGrouped]:
            type R = Window

    given operationAndGrouped: Aux[Operation, Grouped, Operation] =
        new KindOperation[Operation, Grouped]:
            type R = Operation

    given operationAndGroupedOperation: Aux[Operation, GroupedOperation, Operation] =
        new KindOperation[Operation, GroupedOperation]:
            type R = Operation

    given operationAndValue: Aux[Operation, Value, Operation] =
        new KindOperation[Operation, Value]:
            type R = Operation

    given operationAndValueOperation: Aux[Operation, ValueOperation, Operation] =
        new KindOperation[Operation, ValueOperation]:
            type R = Operation

    given aggAndAgg: Aux[Agg, Agg, AggOperation] =
        new KindOperation[Agg, Agg]:
            type R = AggOperation

    given aggAndAggOperation: Aux[Agg, AggOperation, AggOperation] =
        new KindOperation[Agg, AggOperation]:
            type R = AggOperation

    given aggAndWindowEmpty: Aux[Agg, WindowEmpty, WindowAgg] =
        new KindOperation[Agg, WindowEmpty]:
            type R = WindowAgg

    given aggAndWindowAgg: Aux[Agg, WindowAgg, WindowAgg] =
        new KindOperation[Agg, WindowAgg]:
            type R = WindowAgg

    given aggAndWindowGrouped: Aux[Agg, WindowGrouped, WindowAgg] =
        new KindOperation[Agg, WindowGrouped]:
            type R = WindowAgg

    given aggAndGrouped: Aux[Agg, Grouped, AggOperation] =
        new KindOperation[Agg, Grouped]:
            type R = AggOperation

    given aggAndGroupedOperation: Aux[Agg, GroupedOperation, AggOperation] =
        new KindOperation[Agg, GroupedOperation]:
            type R = AggOperation

    given aggAndValue: Aux[Agg, Value, AggOperation] =
        new KindOperation[Agg, Value]:
            type R = AggOperation

    given aggAndValueOperation: Aux[Agg, ValueOperation, AggOperation] =
        new KindOperation[Agg, ValueOperation]:
            type R = AggOperation

    given aggOperationAndAggOperation: Aux[AggOperation, AggOperation, AggOperation] =
        new KindOperation[AggOperation, AggOperation]:
            type R = AggOperation

    given aggOperationAndWindowEmpty: Aux[AggOperation, WindowEmpty, WindowAgg] =
        new KindOperation[AggOperation, WindowEmpty]:
            type R = WindowAgg

    given aggOperationAndWindowAgg: Aux[AggOperation, WindowAgg, WindowAgg] =
        new KindOperation[AggOperation, WindowAgg]:
            type R = WindowAgg

    given aggOperationAndWindowGrouped: Aux[AggOperation, WindowGrouped, WindowAgg] =
        new KindOperation[AggOperation, WindowGrouped]:
            type R = WindowAgg

    given aggOperationAndGrouped: Aux[AggOperation, Grouped, AggOperation] =
        new KindOperation[AggOperation, Grouped]:
            type R = AggOperation

    given aggOperationAndGroupedOperation: Aux[AggOperation, GroupedOperation, AggOperation] =
        new KindOperation[AggOperation, GroupedOperation]:
            type R = AggOperation

    given aggOperationAndValue: Aux[AggOperation, Value, AggOperation] =
        new KindOperation[AggOperation, Value]:
            type R = AggOperation

    given aggOperationAndValueOperation: Aux[AggOperation, ValueOperation, AggOperation] =
        new KindOperation[AggOperation, ValueOperation]:
            type R = AggOperation

    given windowAndWindow: Aux[Window, Window, Window] =
        new KindOperation[Window, Window]:
            type R = Window

    given windowAndWindowEmpty: Aux[Window, WindowEmpty, Window] =
        new KindOperation[Window, WindowEmpty]:
            type R = Window

    given windowAndWindowGrouped: Aux[Window, WindowGrouped, Window] =
        new KindOperation[Window, WindowGrouped]:
            type R = Window

    given windowAndGrouped: Aux[Window, Grouped, Window] =
        new KindOperation[Window, Grouped]:
            type R = Window

    given windowAndGroupedOperation: Aux[Window, GroupedOperation, Window] =
        new KindOperation[Window, GroupedOperation]:
            type R = Window

    given windowAndValue: Aux[Window, Value, Window] =
        new KindOperation[Window, Value]:
            type R = Window

    given windowAndValueOperation: Aux[Window, ValueOperation, Window] =
        new KindOperation[Window, ValueOperation]:
            type R = Window

    given windowEmptyAndWindowEmpty: Aux[WindowEmpty, WindowEmpty, WindowEmpty] =
        new KindOperation[WindowEmpty, WindowEmpty]:
            type R = WindowEmpty

    given windowEmptyAndWindowAgg: Aux[WindowEmpty, WindowAgg, WindowAgg] =
        new KindOperation[WindowEmpty, WindowAgg]:
            type R = WindowAgg

    given windowEmptyAndWindowGrouped: Aux[WindowEmpty, WindowGrouped, WindowGrouped] =
        new KindOperation[WindowEmpty, WindowGrouped]:
            type R = WindowGrouped

    given windowEmptyAndGrouped: Aux[WindowEmpty, Grouped, WindowGrouped] =
        new KindOperation[WindowEmpty, Grouped]:
            type R = WindowGrouped

    given windowEmptyAndGroupedOperation: Aux[WindowEmpty, GroupedOperation, WindowGrouped] =
        new KindOperation[WindowEmpty, GroupedOperation]:
            type R = WindowGrouped

    given windowEmptyAndValue: Aux[WindowEmpty, Value, WindowEmpty] =
        new KindOperation[WindowEmpty, Value]:
            type R = WindowEmpty

    given windowEmptyAndValueOperation: Aux[WindowEmpty, ValueOperation, WindowEmpty] =
        new KindOperation[WindowEmpty, ValueOperation]:
            type R = WindowEmpty

    given windowAggAndWindowAgg: Aux[WindowAgg, WindowAgg, WindowAgg] =
        new KindOperation[WindowAgg, WindowAgg]:
            type R = WindowAgg

    given windowAggAndWindowGrouped: Aux[WindowAgg, WindowGrouped, WindowAgg] =
        new KindOperation[WindowAgg, WindowGrouped]:
            type R = WindowAgg

    given windowAggAndGrouped: Aux[WindowAgg, Grouped, WindowAgg] =
        new KindOperation[WindowAgg, Grouped]:
            type R = WindowAgg

    given windowAggAndGroupedOperation: Aux[WindowAgg, GroupedOperation, WindowAgg] =
        new KindOperation[WindowAgg, GroupedOperation]:
            type R = WindowAgg

    given windowAggAndValue: Aux[WindowAgg, Value, WindowAgg] =
        new KindOperation[WindowAgg, Value]:
            type R = WindowAgg

    given windowAggAndValueOperation: Aux[WindowAgg, ValueOperation, WindowAgg] =
        new KindOperation[WindowAgg, ValueOperation]:
            type R = WindowAgg

    given windowGroupedAndWindowGrouped: Aux[WindowGrouped, WindowGrouped, WindowGrouped] =
        new KindOperation[WindowGrouped, WindowGrouped]:
            type R = WindowGrouped

    given windowGroupedAndGrouped: Aux[WindowGrouped, Grouped, WindowGrouped] =
        new KindOperation[WindowGrouped, Grouped]:
            type R = WindowGrouped

    given windowGroupedAndGroupedOperation: Aux[WindowGrouped, GroupedOperation, WindowGrouped] =
        new KindOperation[WindowGrouped, GroupedOperation]:
            type R = WindowGrouped

    given windowGroupedAndValue: Aux[WindowGrouped, Value, WindowGrouped] =
        new KindOperation[WindowGrouped, Value]:
            type R = WindowGrouped

    given windowGroupedAndValueOperation: Aux[WindowGrouped, ValueOperation, WindowGrouped] =
        new KindOperation[WindowGrouped, ValueOperation]:
            type R = WindowGrouped

    given groupedAndGrouped: Aux[Grouped, Grouped, GroupedOperation] =
        new KindOperation[Grouped, Grouped]:
            type R = GroupedOperation

    given groupedAndGroupedOperation: Aux[Grouped, GroupedOperation, GroupedOperation] =
        new KindOperation[Grouped, GroupedOperation]:
            type R = GroupedOperation

    given groupedAndValue: Aux[Grouped, Value, GroupedOperation] =
        new KindOperation[Grouped, Value]:
            type R = GroupedOperation

    given groupedAndValueOperation: Aux[Grouped, ValueOperation, GroupedOperation] =
        new KindOperation[Grouped, ValueOperation]:
            type R = GroupedOperation

    given groupedOperationAndGroupedOperation: Aux[GroupedOperation, GroupedOperation, GroupedOperation] =
        new KindOperation[GroupedOperation, GroupedOperation]:
            type R = GroupedOperation

    given groupedOperationAndValue: Aux[GroupedOperation, Value, GroupedOperation] =
        new KindOperation[GroupedOperation, Value]:
            type R = GroupedOperation

    given groupedOperationAndValueOperation: Aux[GroupedOperation, ValueOperation, GroupedOperation] =
        new KindOperation[GroupedOperation, ValueOperation]:
            type R = GroupedOperation

    given ungroupedAndUngrouped: Aux[Ungrouped, Ungrouped, Ungrouped] =
        new KindOperation[Ungrouped, Ungrouped]:
            type R = Ungrouped

    given ungroupedAndValue: Aux[Ungrouped, Value, Ungrouped] =
        new KindOperation[Ungrouped, Value]:
            type R = Ungrouped

    given ungroupedAndValueOperation: Aux[Ungrouped, ValueOperation, Ungrouped] =
        new KindOperation[Ungrouped, ValueOperation]:
            type R = Ungrouped

    given valueAndValue: Aux[Value, Value, Value] =
        new KindOperation[Value, Value]:
            type R = Value

    given valueAndValueOperation: Aux[Value, ValueOperation, ValueOperation] =
        new KindOperation[Value, ValueOperation]:
            type R = ValueOperation

    given valueOperationAndValueOperation: Aux[ValueOperation, ValueOperation, ValueOperation] =
        new KindOperation[ValueOperation, ValueOperation]:
            type R = ValueOperation

trait CanInvokeOver[K <: ExprKind]

object CanInvokeOver:
    given window: CanInvokeOver[WindowWithoutOver]()

    given agg: CanInvokeOver[Agg]()

trait CanInAgg[K <: ExprKind]

object CanInAgg:
    given column: CanInAgg[Column]()

    given operation: CanInAgg[Operation]()

    given grouped: CanInAgg[Grouped]()

    given groupedOperation: CanInAgg[GroupedOperation]()

    given ungrouped: CanInAgg[Ungrouped]()

    given value: CanInAgg[Value]()

    given valueOperation: CanInAgg[ValueOperation]()

trait CanInWindow[K <: ExprKind]

object CanInWindow:
    given column: CanInWindow[Column]()

    given operation: CanInWindow[Operation]()

    given grouped: CanInWindow[Grouped]()

    given groupedOperation: CanInWindow[GroupedOperation]()

    given value: CanInWindow[Value]()

    given valueOperation: CanInWindow[ValueOperation]()

trait CanInOver[K <: ExprKind]

object CanInOver:
    given column: CanInOver[Column]()

    given operation: CanInOver[Operation]()

    given agg: CanInOver[Agg]()

    given aggOperation: CanInOver[AggOperation]()

    given grouped: CanInOver[Grouped]()

    given groupedOperation: CanInOver[GroupedOperation]()

    given value: CanInOver[Value]()

    given valueOperation: CanInOver[ValueOperation]()

trait WindowKind[K <: ExprKind]:
    type R <: ExprKind

object WindowKind:
    type Aux[K <: ExprKind, O <: ExprKind] = WindowKind[K]:
        type R = O

    given value: Aux[Value, WindowEmpty] =
        new WindowKind[Value]:
            type R = WindowEmpty

    given valueOperation: Aux[ValueOperation, WindowEmpty] =
        new WindowKind[ValueOperation]:
            type R = WindowEmpty

    given agg[K <: ExprKind : HasAgg]: Aux[K, WindowAgg] =
        new WindowKind[K]:
            type R = WindowAgg

    given grouped: Aux[Grouped, WindowGrouped] =
        new WindowKind[Grouped]:
            type R = WindowGrouped

    given groupedOperation: Aux[GroupedOperation, WindowGrouped] =
        new WindowKind[GroupedOperation]:
            type R = WindowGrouped

    given column: Aux[Column, Window] =
        new WindowKind[Column]:
            type R = Window

    given operation: Aux[Operation, Window] =
        new WindowKind[Operation]:
            type R = Window

trait CanInFilter[K <: ExprKind]

object CanInFilter:
    given column: CanInFilter[Column]()

    given operation: CanInFilter[Operation]()

    given grouped: CanInFilter[Grouped]()

    given groupedOperation: CanInFilter[GroupedOperation]()

    given value: CanInFilter[Value]()

    given valueOperation: CanInFilter[ValueOperation]()

trait CanInGroup[K <: ExprKind]

object CanInGroup:
    given column: CanInGroup[Column]()

    given operation: CanInGroup[Operation]()

    given grouped: CanInGroup[Grouped]()

    given groupedOperation: CanInGroup[GroupedOperation]()

    given valueOperation: CanInGroup[ValueOperation]()

trait CanInMap[K <: ExprKind]:
    type R <: QuerySize

object CanInMap:
    type Aux[K <: ExprKind, RS <: QuerySize] = CanInMap[K]:
        type R = RS

    given column: Aux[Column, ManyRows] =
        new CanInMap[Column]:
            type R = ManyRows

    given operation: Aux[Operation, ManyRows] =
        new CanInMap[Operation]:
            type R = ManyRows

    given window: Aux[Window, ManyRows] =
        new CanInMap[Window]:
            type R = ManyRows

    given windowEmpty: Aux[WindowEmpty, ManyRows] =
        new CanInMap[WindowEmpty]:
            type R = ManyRows

    given windowGrouped: Aux[WindowGrouped, ManyRows] =
        new CanInMap[WindowGrouped]:
            type R = ManyRows

    given grouped: Aux[Grouped, ManyRows] =
        new CanInMap[Grouped]:
            type R = ManyRows

    given groupedOperation: Aux[GroupedOperation, ManyRows] =
        new CanInMap[GroupedOperation]:
            type R = ManyRows

    given value: Aux[Value, ManyRows] =
        new CanInMap[Value]:
            type R = ManyRows

    given valueOperation: Aux[ValueOperation, ManyRows] =
        new CanInMap[ValueOperation]:
            type R = ManyRows

    given agg[K <: ExprKind : HasAgg]: Aux[K, OneRow] =
        new CanInMap[K]:
            type R = OneRow

trait CanInGroupedMap[K <: ExprKind]

object CanInGroupedMap:
    given agg[K <: ExprKind : HasAgg]: CanInGroupedMap[K]()

    given window: CanInGroupedMap[Window]()

    given windowEmpty: CanInGroupedMap[WindowEmpty]()

    given windowGrouped: CanInGroupedMap[WindowGrouped]()

    given grouped: CanInGroupedMap[Grouped]()

    given groupedOperation: CanInGroupedMap[GroupedOperation]()

    given value: CanInGroupedMap[Value]()

    given valueOperation: CanInGroupedMap[ValueOperation]()

trait CanInHaving[K <: ExprKind]

object CanInHaving:
    given agg: CanInHaving[Agg]()

    given aggOperation: CanInHaving[AggOperation]()

    given grouped: CanInHaving[Grouped]()

    given groupedOperation: CanInHaving[GroupedOperation]()

    given value: CanInHaving[Value]()

    given valueOperation: CanInHaving[ValueOperation]()

trait CanInSort[K <: ExprKind, S <: QuerySize]

object CanInSort:
    given columnManyRows: CanInSort[Column, ManyRows]()

    given operationManyRows: CanInSort[Operation, ManyRows]()

    given windowManyRows: CanInSort[Window, ManyRows]()

    given windowEmptyManyRows: CanInSort[WindowEmpty, ManyRows]()

    given windowGroupedManyRows: CanInSort[WindowGrouped, ManyRows]()

    given groupedManyRows: CanInSort[Grouped, ManyRows]()

    given groupedOperationManyRows: CanInSort[GroupedOperation, ManyRows]()

    given valueOperationManyRows: CanInSort[ValueOperation, ManyRows]()

    given aggOneRow[K <: ExprKind : HasAgg]: CanInSort[K, OneRow]()

    given windowEmptyOneRow: CanInSort[WindowEmpty, OneRow]()

    given windowGroupedOneRow: CanInSort[WindowGrouped, OneRow]()

    given groupedOneRow: CanInSort[Grouped, OneRow]()

    given groupedOperationOneRow: CanInSort[GroupedOperation, OneRow]()

    given valueOperationOneRow: CanInSort[ValueOperation, OneRow]()

trait CanInGroupedSort[K <: ExprKind]

object CanInGroupedSort:
    given agg[K <: ExprKind : HasAgg]: CanInGroupedSort[K]()

    given window: CanInGroupedSort[Window]()

    given windowEmpty: CanInGroupedSort[WindowEmpty]()

    given windowGrouped: CanInGroupedSort[WindowGrouped]()

    given grouped: CanInGroupedSort[Grouped]()

    given groupedOperation: CanInGroupedSort[GroupedOperation]()

    given valueOperation: CanInGroupedSort[ValueOperation]()

trait TransformKind[T, K <: ExprKind]:
    type R

    def transform(x: T): R

object TransformKind:
    type Aux[T, K <: ExprKind, O] = TransformKind[T, K]:
        type R = O

    given expr[T, EK <: ExprKind, K <: ExprKind]: Aux[Expr[T, EK], K, Expr[T, K]] =
        new TransformKind[Expr[T, EK], K]:
            type R = Expr[T, K]

            def transform(x: Expr[T, EK]): R =
                Expr(x.asSqlExpr)

    given tuple[H, T <: Tuple, K <: ExprKind](using
        kh: TransformKind[H, K],
        kt: TransformKind[T, K],
        tt: ToTuple[kt.R]
    ): Aux[H *: T, K, kh.R *: tt.R] =
        new TransformKind[H *: T, K]:
            type R = kh.R *: tt.R

            def transform(x: H *: T): R =
                kh.transform(x.head) *: tt.toTuple(kt.transform(x.tail))

    given emptyTuple[K <: ExprKind]: Aux[EmptyTuple, K, EmptyTuple] =
        new TransformKind[EmptyTuple, K]:
            type R = EmptyTuple

            def transform(x: EmptyTuple): R =
                EmptyTuple

    given namedTuple[N <: Tuple, V <: Tuple, K <: ExprKind](using
        k: TransformKind[V, K],
        tt: ToTuple[k.R]
    ): Aux[NamedTuple[N, V], K, NamedTuple[N, tt.R]] =
        new TransformKind[NamedTuple[N, V], K]:
            type R = NamedTuple[N, tt.R]

            def transform(x: NamedTuple[N, V]): R =
                NamedTuple(tt.toTuple(k.transform(x.toTuple)))

trait HasAgg[K <: ExprKind]

object HasAgg:
    given agg: HasAgg[Agg]()

    given aggOperation: HasAgg[AggOperation]()

    given windowAgg: HasAgg[WindowAgg]()

trait IsValue[K <: ExprKind]

object IsValue:
    given value: IsValue[Value]()

    given valueOperation: IsValue[ValueOperation]()

trait TakeQuerySize[N]:
    type R <: QuerySize

object TakeQuerySize:
    type Aux[N, O <: QuerySize] = TakeQuerySize[N]:
        type R = O

    given zero: Aux[0, OneRow] =
        new TakeQuerySize[0]:
            type R = OneRow

    given one: Aux[1, OneRow] =
        new TakeQuerySize[1]:
            type R = OneRow

    given many[N <: Int](using NotGiven[N =:= 0], NotGiven[N =:= 1]): Aux[N, ManyRows] =
        new TakeQuerySize[N]:
            type R = ManyRows


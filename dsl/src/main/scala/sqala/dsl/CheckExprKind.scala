package sqala.dsl

import scala.NamedTuple.NamedTuple
import scala.compiletime.ops.boolean.&&

trait IsAggKind[T]:
    type R <: Boolean

object IsAggKind:
    type Aux[T, O <: Boolean] = IsAggKind[T] { type R = O }

    given notAggKindCheck[T, K <: CommonKind | ColumnKind | WindowKind]: Aux[Expr[T, K], false] =
        new IsAggKind[Expr[T, K]]:
            type R = false

    given aggKindCheck[T, K <: AggKind | ValueKind]: Aux[Expr[T, K], true] =
        new IsAggKind[Expr[T, K]]:
            type R = true

    given tableCheck[T]: Aux[Table[T], false] =
        new IsAggKind[Table[T]]:
            type R = false

    given tupleCheck[H, T <: Tuple](using ch: IsAggKind[H], ct: IsAggKind[T]): Aux[H *: T, ch.R && ct.R] =
        new IsAggKind[H *: T]:
            type R = ch.R && ct.R
    
    given emptyTupleCheck: Aux[EmptyTuple, true] =
        new IsAggKind[EmptyTuple]:
            type R = true

    given namedTupleCheck[N <: Tuple, V <: Tuple](using c: IsAggKind[V]): Aux[NamedTuple[N, V], c.R] =
        new IsAggKind[NamedTuple[N, V]]:
            type R = c.R

trait NotAggKind[T]:
    type R <: Boolean

object NotAggKind:
    type Aux[T, O <: Boolean] = NotAggKind[T] { type R = O }

    given notAggKindCheck[T, K <: CommonKind | ColumnKind | WindowKind | ValueKind]: Aux[Expr[T, K], true] =
        new NotAggKind[Expr[T, K]]:
            type R = true

    given aggKindCheck[T, K <: AggKind]: Aux[Expr[T, K], false] =
        new NotAggKind[Expr[T, K]]:
            type R = false

    given tableCheck[T]: Aux[Table[T], true] =
        new NotAggKind[Table[T]]:
            type R = true

    given tupleCheck[H, T <: Tuple](using ch: NotAggKind[H], ct: NotAggKind[T]): Aux[H *: T, ch.R && ct.R] =
        new NotAggKind[H *: T]:
            type R = ch.R && ct.R
    
    given emptyTupleCheck: Aux[EmptyTuple, true] =
        new NotAggKind[EmptyTuple]:
            type R = true

    given namedTupleCheck[N <: Tuple, V <: Tuple](using c: NotAggKind[V]): Aux[NamedTuple[N, V], c.R] =
        new NotAggKind[NamedTuple[N, V]]:
            type R = c.R

trait NotWindowKind[T]:
    type R <: Boolean

object NotWindowKind:
    type Aux[T, O <: Boolean] = NotWindowKind[T] { type R = O }

    given notWindowKindCheck[T, K <: CommonKind | ColumnKind | ValueKind | AggKind]: Aux[Expr[T, K], true] =
        new NotWindowKind[Expr[T, K]]:
            type R = true

    given windowKindCheck[T, K <: WindowKind]: Aux[Expr[T, K], false] =
        new NotWindowKind[Expr[T, K]]:
            type R = false

    given tableCheck[T]: Aux[Table[T], true] =
        new NotWindowKind[Table[T]]:
            type R = true

    given tupleCheck[H, T <: Tuple](using ch: NotWindowKind[H], ct: NotWindowKind[T]): Aux[H *: T, ch.R && ct.R] =
        new NotWindowKind[H *: T]:
            type R = ch.R && ct.R

    given emptyTupleCheck: Aux[EmptyTuple, true] =
        new NotWindowKind[EmptyTuple]:
            type R = true

    given namedTupleCheck[N <: Tuple, V <: Tuple](using c: NotWindowKind[V]): Aux[NamedTuple[N, V], c.R] =
        new NotWindowKind[NamedTuple[N, V]]:
            type R = c.R

trait ChangeKind[T, K <: ExprKind]:
    type R

    def changeKind(x: T): R

object ChangeKind:
    type Aux[T, K <: ExprKind, O] = ChangeKind[T, K] { type R = O }

    given exprChangeKind[T, EK <: ExprKind, K <: ExprKind]: Aux[Expr[T, EK], K, Expr[T, K]] =
        new ChangeKind[Expr[T, EK], K]:
            type R = Expr[T, K]

            def changeKind(x: Expr[T, EK]): Expr[T, K] = x.asInstanceOf[Expr[T, K]]

    given tupleChangeKind[H, T <: Tuple, K <: ExprKind](using ch: ChangeKind[H, K], ct: ChangeKind[T, K]): Aux[H *: T, K, ch.R *: ToTuple[ct.R]] =
        new ChangeKind[H *: T, K]:
            type R = ch.R *: ToTuple[ct.R]

            def changeKind(x: H *: T): ch.R *: ToTuple[ct.R] =
                val h = ch.changeKind(x.head)
                val t = ct.changeKind(x.tail) match
                    case t: Tuple => t
                    case x => Tuple1(x)
                (h *: t).asInstanceOf[ch.R *: ToTuple[ct.R]]

    given emptyTupleChangeKind[K <: ExprKind]: Aux[EmptyTuple, K, EmptyTuple] =
        new ChangeKind[EmptyTuple, K]:
            type R = EmptyTuple

            def changeKind(x: EmptyTuple): EmptyTuple = x

    given namedTupleChangeKind[N <: Tuple, V <: Tuple, K <: ExprKind](using c: ChangeKind[V, K]): Aux[NamedTuple[N, V], K, NamedTuple[N, ToTuple[c.R]]] =
        new ChangeKind[NamedTuple[N, V], K]:
            type R = NamedTuple[N, ToTuple[c.R]]

            def changeKind(x: NamedTuple[N, V]): NamedTuple[N, ToTuple[c.R]] = 
                val t = c.changeKind(x.toTuple).asInstanceOf[ToTuple[c.R]]
                NamedTuple(t).asInstanceOf[NamedTuple[N, ToTuple[c.R]]]

    given tableChangeKind[T]: Aux[Table[T], ColumnKind, Table[T]] =
        new ChangeKind[Table[T], ColumnKind]:
            type R = Table[T]

            def changeKind(x: Table[T]): Table[T] = x

trait ChangeOption[T]:
    type R

    def changeOption(x: T): R

object ChangeOption:
    type Aux[T, O] = ChangeOption[T] { type R = O }

    given exprChangeOption[T, K <: ExprKind](using a: AsSqlExpr[Wrap[T, Option]]): Aux[Expr[T, K], Expr[Wrap[T, Option], K]] =
        new ChangeOption[Expr[T, K]]:
            type R = Expr[Wrap[T, Option], K]

            def changeOption(x: Expr[T, K]): Expr[Wrap[T, Option], K] = 
                x match
                    case Expr.Literal(v, _) =>
                        val value = v match
                            case o: Option[_] => o
                            case x => Option(x)
                        Expr.Literal(value.asInstanceOf[Wrap[T, Option]], a)
                    case _ => x.asInstanceOf[Expr[Wrap[T, Option], K]]

    given tupleChangeOption[H, T <: Tuple](using ch: ChangeOption[H], ct: ChangeOption[T]): Aux[H *: T, ch.R *: ToTuple[ct.R]] =
        new ChangeOption[H *: T]:
            type R = ch.R *: ToTuple[ct.R]

            def changeOption(x: H *: T): R =
                val h = ch.changeOption(x.head)
                val t = ct.changeOption(x.tail) match
                    case t: Tuple => t
                    case x => Tuple1(x)
                (h *: t).asInstanceOf[ch.R *: ToTuple[ct.R]]

    given emptyTupleChangeOption: Aux[EmptyTuple, EmptyTuple] =
        new ChangeOption[EmptyTuple]:
            type R = EmptyTuple

            def changeOption(x: EmptyTuple): EmptyTuple = x

    given namedTupleChangeOption[N <: Tuple, V <: Tuple](using c: ChangeOption[V]): Aux[NamedTuple[N, V], NamedTuple[N, ToTuple[c.R]]] =
        new ChangeOption[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[c.R]]

            def changeOption(x: NamedTuple[N, V]): NamedTuple[N, ToTuple[c.R]] = 
                val t = c.changeOption(x.toTuple).asInstanceOf[ToTuple[c.R]]
                NamedTuple(t).asInstanceOf[NamedTuple[N, ToTuple[c.R]]]
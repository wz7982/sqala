package sqala.dsl

import sqala.dsl.macros.TableMacro

trait ToOption[T]:
    type R

    def toOption(x: T): R

object ToOption:
    transparent inline given exprToOption[T, K <: ExprKind]: ToOption[Expr[T, K]] = new ToOption[Expr[T, K]]:
        type R = Expr[Wrap[T, Option], K]

        def toOption(x: Expr[T, K]): R = x.asInstanceOf[R]

    transparent inline given tableToOption[X, T <: Table[X]]: ToOption[T] = 
        ${ TableMacro.toOptionTableMacro[X, T] }

    transparent inline given tupleToOption[H, T <: Tuple](using h: ToOption[H], t: ToOption[T]): ToOption[H *: T] = new ToOption[H *: T]:
        type R = h.R *: ToTuple[t.R]

        def toOption(x: H *: T): R =
            (h.toOption(x.head) *: t.toOption(x.tail).asInstanceOf[Tuple]).asInstanceOf[R]

    transparent inline given emptyTupleToOption: ToOption[EmptyTuple] = new ToOption[EmptyTuple]:
        type R = EmptyTuple

        def toOption(x: EmptyTuple): R = x
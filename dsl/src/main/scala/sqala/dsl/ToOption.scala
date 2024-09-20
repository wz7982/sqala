package sqala.dsl

import sqala.dsl.macros.TableMacro
import sqala.dsl.statement.query.SubQuery

import scala.annotation.nowarn

trait ToOption[T]:
    type R

    def toOption(x: T): R

object ToOption:
    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given exprToOption[T, K <: ExprKind]: ToOption[Expr[T, K]] = new ToOption[Expr[T, K]]:
        type R = Expr[Wrap[T, Option], K]

        def toOption(x: Expr[T, K]): R = x.asInstanceOf[R]

    transparent inline given tableToOption[X, T <: Table[X]]: ToOption[T] = 
        ${ TableMacro.toOptionTableMacro[X, T] }

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given subQueryToOption[N <: Tuple, V <: Tuple](using t: ToOption[V]): ToOption[SubQuery[N, V]] =
        new ToOption[SubQuery[N, V]]:
            type R = SubQuery[N, ToTuple[t.R]]

            def toOption(x: SubQuery[N, V]): R =
                x.asInstanceOf[R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tupleToOption[H, T <: Tuple](using h: ToOption[H], t: ToOption[T]): ToOption[H *: T] = new ToOption[H *: T]:
        type R = h.R *: ToTuple[t.R]

        def toOption(x: H *: T): R =
            (h.toOption(x.head) *: t.toOption(x.tail).asInstanceOf[Tuple]).asInstanceOf[R]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given tuple1ToOption[H](using h: ToOption[H]): ToOption[H *: EmptyTuple] = new ToOption[H *: EmptyTuple]:
        type R = h.R *: EmptyTuple

        def toOption(x: H *: EmptyTuple): R =
            (h.toOption(x.head) *: EmptyTuple).asInstanceOf[R]
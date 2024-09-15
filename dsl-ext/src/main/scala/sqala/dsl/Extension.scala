package sqala.dsl

import sqala.ast.statement.SqlSelectItem
import sqala.dsl.statement.query.SelectItem

import scala.NamedTuple.NamedTuple
import scala.annotation.nowarn

object Extension:
    given namedTupleAsExpr[N <: Tuple, V <: Tuple](using a: AsExpr[V]): AsExpr[NamedTuple[N, V]] with
        def asExprs(x: NamedTuple[N, V]): List[Expr[?, ?]] = a.asExprs(x.toTuple)

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given transformNamedTuple[N <: Tuple, V <: Tuple, TK <: ExprKind](using t: TransformKind[NamedTuple[N, V], TK]): TransformKind[NamedTuple[N, V], TK] =
        new TransformKind[NamedTuple[N, V], TK]:
            type R = NamedTuple[N, ToTuple[t.R]]

            def tansform(x: NamedTuple[N, V]): R = 
                NamedTuple(t.tansform(x.toTuple).asInstanceOf[ToTuple[t.R]])

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleHasAgg[N <: Tuple, V <: Tuple](using h: HasAgg[V]): HasAgg[NamedTuple[N, V]] =
        new HasAgg[NamedTuple[N, V]]:
            type R = h.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleHasAgg[N <: Tuple, V <: Tuple](using i: IsAggOrGroup[V]): IsAggOrGroup[NamedTuple[N, V]] =
        new IsAggOrGroup[NamedTuple[N, V]]:
            type R = i.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleNotAgg[N <: Tuple, V <: Tuple](using n: NotAgg[V]): NotAgg[NamedTuple[N, V]] =
        new NotAgg[NamedTuple[N, V]]:
            type R = n.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleNotWindow[N <: Tuple, V <: Tuple](using n: NotWindow[V]): NotWindow[NamedTuple[N, V]] =
        new NotWindow[NamedTuple[N, V]]:
            type R = n.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleNotValue[N <: Tuple, V <: Tuple](using n: NotValue[V]): NotValue[NamedTuple[N, V]] =
        new NotValue[NamedTuple[N, V]]:
            type R = n.R

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleUnion[LN <: Tuple, LV <: Tuple, RN <: Tuple, RV <: Tuple](using u: UnionOperation[LV, RV]): UnionOperation[NamedTuple[LN, LV], NamedTuple[RN, RV]] = 
        new UnionOperation[NamedTuple[LN, LV], NamedTuple[RN, RV]]:
            type R = NamedTuple[LN, ToTuple[u.R]]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleResult[N <: Tuple, V <: Tuple](using r: Result[V]): Result[NamedTuple[N, V]] =
        new Result[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[r.R]]

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleToOption[N <: Tuple, V <: Tuple](using t: ToOption[V]): ToOption[NamedTuple[N, V]] = 
        new ToOption[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[t.R]]

            def toOption(x: NamedTuple[N, V]): R =
                NamedTuple(t.toOption(x.toTuple).asInstanceOf[ToTuple[t.R]])

    @nowarn("msg=New anonymous class definition will be duplicated at each inline site")
    transparent inline given namedTupleSelectItem[N <: Tuple, V <: Tuple](using s: SelectItem[V]): SelectItem[NamedTuple[N, V]] = 
        new SelectItem[NamedTuple[N, V]]:
            type R = NamedTuple[N, ToTuple[s.R]]

            def subQueryItems(item: NamedTuple[N, V], cursor: Int, alias: String): R =
                NamedTuple(s.subQueryItems(item.toTuple, cursor, alias).asInstanceOf[ToTuple[s.R]])

            def offset(item: NamedTuple[N, V]): Int = s.offset(item.toTuple)

            def selectItems(item: NamedTuple[N, V], cursor: Int): List[SqlSelectItem.Item] =
                s.selectItems(item.toTuple, cursor)
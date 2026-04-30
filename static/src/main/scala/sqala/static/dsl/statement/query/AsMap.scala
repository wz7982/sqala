package sqala.static.dsl.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.static.dsl.*
import sqala.static.dsl.table.*
import sqala.static.metadata.AsSqlExpr

import scala.NamedTuple.NamedTuple

trait AsSelect[T]:
    type R

    def transform(x: T): R

    def offset(x: T): Int

    def asSelectItems(x: T, cursor: Int): List[SqlSelectItem.Expr]

object AsSelect:
    type Aux[T, O] = AsSelect[T]:
        type R = O

    given table[T]: Aux[Table[T, Column, CanNotInFrom], Table[T, Column, CanNotInFrom]] =
        new AsSelect[Table[T, Column, CanNotInFrom]]:
            type R = Table[T, Column, CanNotInFrom]

            def transform(x: Table[T, Column, CanNotInFrom]): R =
                x

            def offset(x: Table[T, Column, CanNotInFrom]): Int =
                x.__metaData__.fieldNames.size

            def asSelectItems(x: Table[T, Column, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                for
                    (column, index) <- x.__metaData__.columnNames.zipWithIndex
                yield
                    SqlSelectItem.Expr(
                        SqlExpr.Column(x.__aliasName__, column), Some(s"c${cursor + index}")
                    )

    given jsonTable[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[JsonTable[N, V, CanNotInFrom], JsonTable[N, t.R, CanNotInFrom]] =
        new AsSelect[JsonTable[N, V, CanNotInFrom]]:
            type R = JsonTable[N, t.R, CanNotInFrom]

            def transform(x: JsonTable[N, V, CanNotInFrom]): R =
                new JsonTable(
                    x.__aliasName__,
                    t.toTuple(s.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: JsonTable[N, V, CanNotInFrom]): Int =
                s.offset(x.__items__)

            def asSelectItems(x: JsonTable[N, V, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.__items__, cursor)

    given funcTable[T]: Aux[FuncTable[T, Column, CanNotInFrom], FuncTable[T, Column, CanNotInFrom]] =
        new AsSelect[FuncTable[T, Column, CanNotInFrom]]:
            type R = FuncTable[T, Column, CanNotInFrom]

            def transform(x: FuncTable[T, Column, CanNotInFrom]): R =
                x

            def offset(x: FuncTable[T, Column, CanNotInFrom]): Int =
                x.__fieldNames__.size

            def asSelectItems(x: FuncTable[T, Column, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                for
                    (column, index) <- x.__columnNames__.zipWithIndex
                yield
                    SqlSelectItem.Expr(
                        SqlExpr.Column(x.__aliasName__, column), Some(s"c${cursor + index}")
                    )

    given excludedTable[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[ExcludedTable[N, V, CanNotInFrom], ExcludedTable[N, t.R, CanNotInFrom]] =
        new AsSelect[ExcludedTable[N, V, CanNotInFrom]]:
            type R = ExcludedTable[N, t.R, CanNotInFrom]

            def transform(x: ExcludedTable[N, V, CanNotInFrom]): R =
                new ExcludedTable(
                    x.__aliasName__,
                    t.toTuple(s.transform(x.__items__.asInstanceOf[V])),
                    x.__sqlTable__
                )

            def offset(x: ExcludedTable[N, V, CanNotInFrom]): Int =
                s.offset(x.__items__.asInstanceOf[V])

            def asSelectItems(x: ExcludedTable[N, V, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.__items__.asInstanceOf[V], cursor)

    given graphTable[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[GraphTable[N, V, CanNotInFrom], GraphTable[N, t.R, CanNotInFrom]] =
        new AsSelect[GraphTable[N, V, CanNotInFrom]]:
            type R = GraphTable[N, t.R, CanNotInFrom]

            def transform(x: GraphTable[N, V, CanNotInFrom]): R =
                new GraphTable(
                    x.__aliasName__,
                    t.toTuple(s.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: GraphTable[N, V, CanNotInFrom]): Int =
                s.offset(x.__items__)

            def asSelectItems(x: GraphTable[N, V, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.__items__, cursor)

    given recursiveTable[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[RecursiveTable[N, V], RecursiveTable[N, t.R]] =
        new AsSelect[RecursiveTable[N, V]]:
            type R = RecursiveTable[N, t.R]

            def transform(x: RecursiveTable[N, V]): R =
                new RecursiveTable(
                    x.__aliasName__,
                    t.toTuple(s.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: RecursiveTable[N, V]): Int =
                s.offset(x.__items__)

            def asSelectItems(x: RecursiveTable[N, V], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.__items__, cursor)

    given recognizeTable[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[RecognizeTable[N, V, CanNotInFrom], RecognizeTable[N, t.R, CanNotInFrom]] =
        new AsSelect[RecognizeTable[N, V, CanNotInFrom]]:
            type R = RecognizeTable[N, t.R, CanNotInFrom]

            def transform(x: RecognizeTable[N, V, CanNotInFrom]): R =
                new RecognizeTable(
                    x.__aliasName__,
                    t.toTuple(s.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: RecognizeTable[N, V, CanNotInFrom]): Int =
                s.offset(x.__items__)

            def asSelectItems(x: RecognizeTable[N, V, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.__items__, cursor)

    given subQueryTable[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[SubQueryTable[N, V, CanNotInFrom], SubQueryTable[N, t.R, CanNotInFrom]] =
        new AsSelect[SubQueryTable[N, V, CanNotInFrom]]:
            type R = SubQueryTable[N, t.R, CanNotInFrom]

            def transform(x: SubQueryTable[N, V, CanNotInFrom]): R =
                new SubQueryTable(
                    x.__aliasName__,
                    t.toTuple(s.transform(x.__items__)),
                    x.__sqlTable__
                )

            def offset(x: SubQueryTable[N, V, CanNotInFrom]): Int =
                s.offset(x.__items__)

            def asSelectItems(x: SubQueryTable[N, V, CanNotInFrom], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.__items__, cursor)

    given tuple[H, T <: Tuple](using
        sh: AsSelect[H],
        st: AsSelect[T],
        tt: ToTuple[st.R]
    ): Aux[H *: T, sh.R *: tt.R] =
        new AsSelect[H *: T]:
            type R = sh.R *: tt.R

            def transform(x: H *: T): R =
                sh.transform(x.head) *: tt.toTuple(st.transform(x.tail))

            def offset(x: H *: T): Int =
                sh.offset(x.head) + st.offset(x.tail)

            def asSelectItems(x: H *: T, cursor: Int): List[SqlSelectItem.Expr] =
                sh.asSelectItems(x.head, cursor) ++ st.asSelectItems(x.tail, cursor + sh.offset(x.head))

    given tuple1[H](using
        sh: AsSelect[H]
    ): Aux[H *: EmptyTuple, sh.R *: EmptyTuple] =
        new AsSelect[H *: EmptyTuple]:
            type R = sh.R *: EmptyTuple

            def transform(x: H *: EmptyTuple): R =
                sh.transform(x.head) *: EmptyTuple

            def offset(x: H *: EmptyTuple): Int =
                sh.offset(x.head)

            def asSelectItems(x: H *: EmptyTuple, cursor: Int): List[SqlSelectItem.Expr] =
                sh.asSelectItems(x.head, cursor)

trait AsMap[T]:
    type R

    type K <: ExprKind

    def transform(x: T): R

    def offset(x: T): Int

    def asSelectItems(x: T, cursor: Int): List[SqlSelectItem.Expr]

object AsMap:
    type Aux[T, O, OK <: ExprKind] = AsMap[T]:
        type R = O

        type K = OK

    given expr[T: AsSqlExpr, EK <: ExprKind]: Aux[Expr[T, EK], Expr[T, Column], EK] =
        new AsMap[Expr[T, EK]]:
            type R = Expr[T, Column]

            type K = EK

            def transform(x: Expr[T, K]): R =
                Expr(x.asSqlExpr)

            def offset(x: Expr[T, K]): Int =
                1

            def asSelectItems(x: Expr[T, K], cursor: Int): List[SqlSelectItem.Expr] =
                SqlSelectItem.Expr(x.asSqlExpr, Some(s"c$cursor")) :: Nil

    given value[T: AsSqlExpr as a]: Aux[T, Expr[T, Column], Value] =
        new AsMap[T]:
            type R = Expr[T, Column]

            type K = Value

            def transform(x: T): R =
                Expr(a.asSqlExpr(x))

            def offset(x: T): Int =
                1

            def asSelectItems(x: T, cursor: Int): List[SqlSelectItem.Expr] =
                SqlSelectItem.Expr(transform(x).asSqlExpr, Some(s"c$cursor")) :: Nil

    given scalarQuery[T: AsSqlExpr, EK <: ExprKind, Q <: Query[Expr[T, EK], OneRow]]: Aux[Q, Expr[T, Column], ValueOperation] =
        new AsMap[Q]:
            type R = Expr[T, Column]

            type K = ValueOperation

            def transform(x: Q): R =
                Expr(SqlExpr.SubQuery(x.tree))

            def offset(x: Q): Int =
                1

            def asSelectItems(x: Q, cursor: Int): List[SqlSelectItem.Expr] =
                SqlSelectItem.Expr(transform(x).asSqlExpr, Some(s"c$cursor")) :: Nil

    given tuple[H, T <: Tuple](using
        sh: AsMap[H],
        st: AsMap[T],
        tt: ToTuple[st.R],
        a: AsExpr[sh.R],
        as: AsSqlExpr[a.R],
        o: KindOperation[sh.K, st.K]
    ): Aux[H *: T, sh.R *: tt.R, o.R] =
        new AsMap[H *: T]:
            type R = sh.R *: tt.R

            type K = o.R

            def transform(x: H *: T): R =
                sh.transform(x.head) *: tt.toTuple(st.transform(x.tail))

            def offset(x: H *: T): Int =
                sh.offset(x.head) + st.offset(x.tail)

            def asSelectItems(x: H *: T, cursor: Int): List[SqlSelectItem.Expr] =
                sh.asSelectItems(x.head, cursor) ++ st.asSelectItems(x.tail, cursor + sh.offset(x.head))

    given tuple1[H](using
        sh: AsMap[H],
        a: AsExpr[sh.R],
        as: AsSqlExpr[a.R],
        o: KindOperation[sh.K, Value]
    ): Aux[H *: EmptyTuple, sh.R *: EmptyTuple, o.R] =
        new AsMap[H *: EmptyTuple]:
            type R = sh.R *: EmptyTuple

            type K = o.R

            def transform(x: H *: EmptyTuple): R =
                sh.transform(x.head) *: EmptyTuple

            def offset(x: H *: EmptyTuple): Int = sh.offset(x.head)

            def asSelectItems(x: H *: EmptyTuple, cursor: Int): List[SqlSelectItem.Expr] =
                sh.asSelectItems(x.head, cursor)

    given namedTuple[N <: Tuple, V <: Tuple](using
        s: AsMap[V],
        t: ToTuple[s.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, t.R], s.K] =
        new AsMap[NamedTuple[N, V]]:
            type R = NamedTuple[N, t.R]

            type K = s.K

            def transform(x: NamedTuple[N, V]): R =
                t.toTuple(s.transform(x.toTuple))

            def offset(x: NamedTuple[N, V]): Int = s.offset(x.toTuple)

            def asSelectItems(x: NamedTuple[N, V], cursor: Int): List[SqlSelectItem.Expr] =
                s.asSelectItems(x.toTuple, cursor)
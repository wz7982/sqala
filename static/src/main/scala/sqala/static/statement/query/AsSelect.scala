package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlSelectItem
import sqala.static.dsl.*

import scala.NamedTuple.NamedTuple
import scala.annotation.implicitNotFound
import scala.collection.mutable.ListBuffer

@implicitNotFound("Type ${T} cannot be converted to SELECT items.")
trait AsSelect[T]:
    type R

    def transform(x: T): R

    def offset(x: T): Int

    def selectItems(x: T, cursor: Int): List[SqlSelectItem.Item]

object AsSelect:
    type Aux[T, O] = AsSelect[T]:
        type R = O

    given expr[T]: Aux[Expr[T], Expr[T]] = new AsSelect[Expr[T]]:
        type R = Expr[T]

        def transform(x: Expr[T]): R = x

        def offset(x: Expr[T]): Int = 1

        def selectItems(x: Expr[T], cursor: Int): List[SqlSelectItem.Item] =
            SqlSelectItem.Item(x.asSqlExpr, Some(s"c${cursor}")) :: Nil

    given value[T: AsSqlExpr as a]: Aux[T, Expr[T]] = new AsSelect[T]:
        type R = Expr[T]

        def transform(x: T): R = Expr.Literal(x, a)

        def offset(x: T): Int = 1

        def selectItems(x: T, cursor: Int): List[SqlSelectItem.Item] =
            SqlSelectItem.Item(transform(x).asSqlExpr, Some(s"c${cursor}")) :: Nil

    given table[T]: Aux[Table[T], Table[T]] = new AsSelect[Table[T]]:
        type R = Table[T]

        def transform(x: Table[T]): R = x

        def offset(x: Table[T]): Int = x.__metaData__.fieldNames.size

        def selectItems(x: Table[T], cursor: Int): List[SqlSelectItem.Item] =
            var tmpCursor = cursor
            val items = ListBuffer[SqlSelectItem.Item]()
            for field <- x.__metaData__.columnNames do
                items.addOne(
                    SqlSelectItem.Item(
                        SqlExpr.Column(Some(x.__aliasName__), field), Some(s"c${tmpCursor}")
                    )
                )
                tmpCursor += 1
            items.toList

    given scalarQuery[T: AsSqlExpr]: Aux[Query[Expr[T]], Expr[T]] = new AsSelect[Query[Expr[T]]]:
        type R = Expr[T]

        def transform(x: Query[Expr[T]]): R = x.asExpr

        def offset(x: Query[Expr[T]]): Int = 1

        def selectItems(x: Query[Expr[T]], cursor: Int): List[SqlSelectItem.Item] =
            SqlSelectItem.Item(transform(x).asSqlExpr, Some(s"c${cursor}")) :: Nil

    given subQuery[N <: Tuple, V <: Tuple](using 
        s: AsSelect[V],
        t: ToTuple[s.R]
    ): Aux[SubQuery[N, V], SubQuery[N, t.R]] = new AsSelect[SubQuery[N, V]]:
        type R = SubQuery[N, t.R]

        def transform(x: SubQuery[N, V]): R =
            new SubQuery(x.__alias__, t.toTuple(s.transform(x.__items__)))(using x.context)

        def offset(x: SubQuery[N, V]): Int = s.offset(x.__items__)

        def selectItems(x: SubQuery[N, V], cursor: Int): List[SqlSelectItem.Item] =
            s.selectItems(x.__items__, cursor)

    given tuple[H, T <: Tuple](using 
        sh: AsSelect[H],
        st: AsSelect[T],
        tt: ToTuple[st.R]
    ): Aux[H *: T, sh.R *: tt.R] = new AsSelect[H *: T]:
        type R = sh.R *: tt.R

        def transform(x: H *: T): R = 
            sh.transform(x.head) *: tt.toTuple(st.transform(x.tail))

        def offset(x: H *: T): Int = 
            sh.offset(x.head) + st.offset(x.tail)

        def selectItems(x: H *: T, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(x.head, cursor) ++ st.selectItems(x.tail, cursor + sh.offset(x.head))

    given tuple1[H](using 
        sh: AsSelect[H]
    ): Aux[H *: EmptyTuple, sh.R *: EmptyTuple] = new AsSelect[H *: EmptyTuple]:
        type R = sh.R *: EmptyTuple

        def transform(x: H *: EmptyTuple): R =
            sh.transform(x.head) *: EmptyTuple

        def offset(x: H *: EmptyTuple): Int = sh.offset(x.head)

        def selectItems(x: H *: EmptyTuple, cursor: Int): List[SqlSelectItem.Item] =
            sh.selectItems(x.head, cursor)

    given namedTuple[N <: Tuple, V <: Tuple](using
        s: AsSelect[V],
        t: ToTuple[s.R]
    ): Aux[NamedTuple[N, V], NamedTuple[N, t.R]] = new AsSelect[NamedTuple[N, V]]:
        type R = NamedTuple[N, t.R]

        def transform(x: NamedTuple[N, V]): R =
            t.toTuple(s.transform(x.toTuple))

        def offset(x: NamedTuple[N, V]): Int = s.offset(x.toTuple)

        def selectItems(x: NamedTuple[N, V], cursor: Int): List[SqlSelectItem.Item] =
            s.selectItems(x.toTuple, cursor)
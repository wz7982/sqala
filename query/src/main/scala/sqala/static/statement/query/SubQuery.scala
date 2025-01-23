package sqala.static.statement.query

import sqala.common.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.*
import scala.compiletime.constValue

class SubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __alias__ : String,
    private[sqala] val __items__ : V
)(using 
    private[sqala] val __context__ : QueryContext
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object SubQuery:
    def apply[N <: Tuple, V <: Tuple](items: NamedTuple[N, V])(using 
        s: AsSubQuery[V],
        qc: QueryContext
    ): SubQuery[N, V] =
        qc.tableIndex += 1
        val alias = s"t${qc.tableIndex}"
        new SubQuery(alias, s.asSubQuery(items, alias, 1))

    def apply[N <: Tuple, V <: Tuple](items: NamedTuple[N, V], alias: String)(using 
        s: AsSubQuery[V],
        qc: QueryContext
    ): SubQuery[N, V] =
        new SubQuery(alias, s.asSubQuery(items, alias, 1))

trait AsSubQuery[T]:
    def offset(x: T): Int

    def asSubQuery(x: T, queryAlias: String, cursor: Int): T

object AsSubQuery:
    given expr[T]: AsSubQuery[Expr[T]] with
        def offset(x: Expr[T]): Int = 1

        def asSubQuery(x: Expr[T], queryAlias: String, cursor: Int): Expr[T] =
            Expr.Column(queryAlias, s"c$cursor")

    given table[T]: AsSubQuery[Table[T]] with
        def offset(x: Table[T]): Int = x.__metaData__.columnNames.size

        def asSubQuery(x: Table[T], queryAlias: String, cursor: Int): Table[T] =
            val fields = (cursor to cursor + x.__metaData__.columnNames.size)
                .toList
                .map(i => s"c$i")
            val metaData = TableMetaData(
                queryAlias, 
                Nil, 
                None, 
                fields,
                fields
            )
            Table(queryAlias, queryAlias, metaData)

    given subQuery[N <: Tuple, V <: Tuple](using 
        s: AsSubQuery[V]
    ): AsSubQuery[SubQuery[N, V]] with
        def offset(item: SubQuery[N, V]): Int = s.offset(item.__items__)

        def asSubQuery(x: SubQuery[N, V], queryAlias: String, cursor: Int): SubQuery[N, V] =
            SubQuery(s.asSubQuery(x.__items__, queryAlias, cursor))(using s, x.__context__)

    given tuple[H, T <: Tuple](using
        h: AsSubQuery[H],
        t: AsSubQuery[T]
    ): AsSubQuery[H *: T] with
        def offset(x: H *: T): Int = 
            h.offset(x.head) + t.offset(x.tail)

        def asSubQuery(x: H *: T, queryAlias: String, cursor: Int): H *: T =
            h.asSubQuery(x.head, queryAlias, cursor) *:
            t.asSubQuery(x.tail, queryAlias, cursor + h.offset(x.head))

    given tuple1[H](using h: AsSubQuery[H]): AsSubQuery[H *: EmptyTuple] with
        def offset(x: H *: EmptyTuple): Int = h.offset(x.head)

        def asSubQuery(x: H *: EmptyTuple, queryAlias: String, cursor: Int): H *: EmptyTuple =
            h.asSubQuery(x.head, queryAlias, cursor) *: EmptyTuple
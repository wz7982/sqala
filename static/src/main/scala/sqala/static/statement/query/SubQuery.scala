package sqala.static.statement.query

import sqala.static.common.*

import scala.NamedTuple.*

class SubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __columns__ : List[String]
) extends Selectable:
    type Fields = NamedTuple[N, V]

    def selectDynamic(name: String): Any = compileTimeOnly

class TableSubQuery[T](
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Any = compileTimeOnly
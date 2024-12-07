package sqala.static.common

import scala.NamedTuple.*

class Table[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Any = compileTimeOnly

object Table:
    extension [T](table: Table[T])
        def * : table.Fields = compileTimeOnly
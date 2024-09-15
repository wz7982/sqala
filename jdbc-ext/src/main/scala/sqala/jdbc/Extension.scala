package sqala.jdbc

import java.sql.ResultSet
import scala.NamedTuple.NamedTuple

object Extension:
    given namedTupleDecoder[N <: Tuple, V <: Tuple](using d: JdbcDecoder[V]): JdbcDecoder[NamedTuple[N, V]] with
        override inline def offset: Int = d.offset

        override inline def decode(data: ResultSet, cursor: Int): NamedTuple[N, V] =
            NamedTuple(d.decode(data, cursor))
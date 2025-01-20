package sqala.static.dsl

import sqala.common.TableMetaData

import scala.NamedTuple.*
import scala.compiletime.constValue

case class Table[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    inline def selectDynamic(name: String): Expr[?] =
        val index = 
            constValue[Index[Names[From[Unwrap[T, Option]]], name.type, 0]]
        Expr.Column(__aliasName__, __metaData__.columnNames(index))
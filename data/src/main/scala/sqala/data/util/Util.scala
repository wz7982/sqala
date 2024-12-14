package sqala.data.util

import scala.compiletime.{constValue, erasedValue}
import scala.quoted.*

private[sqala] inline def fetchNames[T <: Tuple]: List[String] =
    inline erasedValue[T] match
        case _: EmptyTuple => Nil
        case _: (x *: xs) => constValue[x].asInstanceOf[String] :: fetchNames[xs]

private[sqala] def fetchTypes[T: Type](using q: Quotes): List[Type[?]] =
    Type.of[T] match
        case '[x *: xs] => Type.of[x] :: fetchTypes[xs]
        case '[EmptyTuple] => Nil
package sqala.util

import scala.compiletime.{constValue, erasedValue}

private[sqala] inline def fetchNames[T <: Tuple]: List[String] =
    inline erasedValue[T] match
        case _: EmptyTuple => Nil
        case _: (x *: xs) => constValue[x].asInstanceOf[String] :: fetchNames[xs]
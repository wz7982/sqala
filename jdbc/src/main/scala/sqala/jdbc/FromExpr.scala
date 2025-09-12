package sqala.jdbc

import scala.quoted.*

given [T](using Type[T]): FromExpr[SetDefaultValue[T]] with
    override def unapply(x: Expr[SetDefaultValue[T]])(using Quotes): Option[SetDefaultValue[T]] = x match
        case '{ SetDefaultValue.setInt } => Some(SetDefaultValue.setInt.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setLong } => Some(SetDefaultValue.setLong.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setFloat } => Some(SetDefaultValue.setFloat.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setDouble } => Some(SetDefaultValue.setDouble.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setDecimal } => Some(SetDefaultValue.setDecimal.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setBoolean } => Some(SetDefaultValue.setBoolean.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setString } => Some(SetDefaultValue.setString.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setLocalDate } => Some(SetDefaultValue.setLocalDate.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setLocalDateTime } => Some(SetDefaultValue.setLocalDateTime.asInstanceOf[SetDefaultValue[T]])
        case _ => None
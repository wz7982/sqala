package sqala.dsl

type Wrap[T, F[_]] <: F[?] = T match
    case F[t] => F[t]
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => t
    case _ => T

type MapField[X, T] = T match
    case Option[_] => Wrap[X, Option]
    case _ => X

type HasOption[L, R] = (L, R) match
    case (Option[_], _) => true
    case (_, Option[_]) => true
    case _ => false

type OperationResult[L, R, T] = HasOption[L, R] match
    case true => Option[T]
    case false => T

type NumericOperationResult[L, R] = (Unwrap[L, Option], Unwrap[R, Option]) match
    case (BigDecimal, _) => OperationResult[L, R, BigDecimal]
    case (_, BigDecimal) => OperationResult[L, R, BigDecimal]
    case (Double, _) => OperationResult[L, R, Double]
    case (_, Double) => OperationResult[L, R, Double]
    case (Float, _) => OperationResult[L, R, Float]
    case (_, Float) => OperationResult[L, R, Float]
    case (Long, _) => OperationResult[L, R, Long]
    case (_, Long) => OperationResult[L, R, Long]
    case _ => OperationResult[L, R, Int]
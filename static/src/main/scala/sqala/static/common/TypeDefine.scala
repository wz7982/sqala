package sqala.static.common

type Wrap[T, F[_]] = T match
    case F[t] => F[t]
    case _ => F[T]

type Unwrap[T, F[_]] = T match
    case F[t] => Unwrap[t, F]
    case _ => T

type MapField[X, T] = T match
    case Option[_] => Wrap[X, Option]
    case _ => X

type HasOption[L, R] = (L, R) match
    case (Option[_], _) => true
    case (_, Option[_]) => true
    case _ => false

type OperationResult[N <: Boolean, T] = N match
    case true => Option[T]
    case false => T

type NumericResult[L, R, N <: Boolean] = (Unwrap[L, Option], Unwrap[R, Option]) match
    case (BigDecimal, _) => OperationResult[N, BigDecimal]
    case (_, BigDecimal) => OperationResult[N, BigDecimal]
    case (Double, _) => OperationResult[N, Double]
    case (_, Double) => OperationResult[N, Double]
    case (Float, _) => OperationResult[N, Float]
    case (_, Float) => OperationResult[N, Float]
    case (Long, _) => OperationResult[N, Long]
    case (_, Long) => OperationResult[N, Long]
    case _ => OperationResult[N, Int]
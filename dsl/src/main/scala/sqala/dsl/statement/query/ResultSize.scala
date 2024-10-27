package sqala.dsl.statement.query

import scala.util.NotGiven

enum ResultSize:
    case OneRow
    case ManyRows

type OneRow = ResultSize.OneRow.type

type ManyRows = ResultSize.ManyRows.type

trait QuerySize[T <: Int]:
    type R <: ResultSize

object QuerySize:
    type Aux[T <: Int, O <: ResultSize] = QuerySize[T]:
        type R = O

    given one: Aux[1, OneRow] = new QuerySize[1]:
        type R = OneRow

    given many[T <: Int](using NotGiven[T =:= 1]): Aux[T, ManyRows] = new QuerySize[T]:
        type R = ManyRows

trait ProjectionSize[IsAgg <: Boolean]:
    type R <: ResultSize

object ProjectionSize:
    type Aux[IsAgg <: Boolean, O <: ResultSize] = ProjectionSize[IsAgg]:
        type R = O

    given one: Aux[true, OneRow] = new ProjectionSize[true]:
        type R = OneRow

    given many: Aux[false, ManyRows] = new ProjectionSize[false]:
        type R = ManyRows
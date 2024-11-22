package sqala.dsl

enum ResultSize:
    case OneRow
    case ManyRows

type OneRow = ResultSize.OneRow.type

type ManyRows = ResultSize.ManyRows.type

class Query[T, S <: ResultSize]
package sqala.static.statement.dml

enum InsertState:
    case New
    case Entity
    case Table
    case Values
    case Query

type InsertNew = InsertState.New.type

type InsertEntity = InsertState.Entity.type

type InsertTable = InsertState.Table.type

type InsertValues = InsertState.Values.type

type InsertQuery = InsertState.Query.type
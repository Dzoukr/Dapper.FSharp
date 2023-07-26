[<AutoOpen>]
module Dapper.FSharp.SQLite.Domain

type OrderDirection =
    | Asc
    | Desc

type OrderBy = string * OrderDirection

type ColumnComparison =
    | Eq of obj
    | Ne of obj
    | Gt of obj
    | Lt of obj
    | Ge of obj
    | Le of obj
    | In of obj list
    | NotIn of obj list
    | Like of string
    | NotLike of string
    | IsNull
    | IsNotNull

type BinaryOperation =
    | And
    | Or

type UnaryOperation =
    | Not

type Where =
    | Empty
    | Column of string * ColumnComparison
    | Binary of Where * BinaryOperation * Where
    | Unary of UnaryOperation * Where
    | Expr of string
    static member (+) (a, b) = Binary(a, And, b)
    static member (*) (a, b) = Binary(a, Or, b)
    static member (!!) a = Unary (Not, a)

type Pagination = {
    Skip : int
    Take : int
}

type JoinType =
    | EqualsToColumn of string
    | EqualsToConstant of obj

type Join =
    | InnerJoin of table:string * List<string * JoinType>
    | LeftJoin of table:string * List<string * JoinType>

module Join =
    let tableName = function
        | InnerJoin (t, _)
        | LeftJoin (t, _) -> t

type Aggregate =
    | Count of columnName:string * alias:string
    | CountDistinct of columnName:string * alias:string
    | Avg of columnName:string * alias:string
    | Sum of columnName:string * alias:string
    | Min of columnName:string * alias:string
    | Max of columnName:string * alias:string

type SelectQuery = {
    Table : string
    Where : Where
    OrderBy : OrderBy list
    Pagination : Pagination
    Joins : Join list
    Aggregates : Aggregate list
    GroupBy : string list
    Distinct : bool
}

type InsertQuery<'a> = {
    Table : string
    Fields : string list
    Values : 'a list
}

type UpdateQuery<'a> = {
    Table : string
    Value : 'a option
    SetColumns: (string * obj) list
    Fields : string list
    Where : Where
}

type DeleteQuery = {
    Table : string
    Where : Where
}
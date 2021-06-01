namespace Dapper.FSharp

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
    Take : int option
}
    
type Join =
    | InnerJoin of table:string * colName:string * equalsToColumn:string
    | LeftJoin of table:string * colName:string * equalsToColumn:string

module Join =
    let tableName = function
        | InnerJoin (t,_,_)
        | LeftJoin (t,_,_) -> t

type Aggregate =
    | Count of columnName:string * alias:string
    | Avg of columnName:string * alias:string
    | Sum of columnName:string * alias:string
    | Min of columnName:string * alias:string
    | Max of columnName:string * alias:string

type SelectQuery = {
    Schema : string option
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
    Schema : string option
    Table : string
    Fields : string list option
    Values : 'a list
}

type UpdateQuery<'a> = {
    Schema : string option
    Table : string
    Value : 'a
    Fields : string list option
    Where : Where
}

type DeleteQuery = {
    Schema : string option
    Table : string
    Where : Where
}
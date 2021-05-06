module Dapper.FSharp.ExpressionBuilders

open System.Linq.Expressions
open System

let (|Lambda|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Lambda -> Some (exp :?> LambdaExpression)
    | _ -> None

let (|Unary|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.ArrayLength
    | ExpressionType.Convert
    | ExpressionType.ConvertChecked
    | ExpressionType.Negate
    | ExpressionType.UnaryPlus
    | ExpressionType.NegateChecked
    | ExpressionType.Not
    | ExpressionType.Quote
    | ExpressionType.TypeAs -> Some (exp :?> UnaryExpression)
    | _ -> None

let (|Binary|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Add
    | ExpressionType.AddChecked
    | ExpressionType.And
    | ExpressionType.AndAlso
    | ExpressionType.ArrayIndex
    | ExpressionType.Coalesce
    | ExpressionType.Divide
    | ExpressionType.Equal
    | ExpressionType.ExclusiveOr
    | ExpressionType.GreaterThan
    | ExpressionType.GreaterThanOrEqual
    | ExpressionType.LeftShift
    | ExpressionType.LessThan
    | ExpressionType.LessThanOrEqual
    | ExpressionType.Modulo
    | ExpressionType.Multiply
    | ExpressionType.MultiplyChecked
    | ExpressionType.NotEqual
    | ExpressionType.Or
    | ExpressionType.OrElse
    | ExpressionType.Power
    | ExpressionType.RightShift
    | ExpressionType.Subtract
    | ExpressionType.SubtractChecked -> Some (exp :?> BinaryExpression)
    | _ -> None

let (|MethodCall|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Call -> Some (exp :?> MethodCallExpression)
    | _ -> None

let (|Constant|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Constant -> Some (exp :?> ConstantExpression)
    | _ -> None

let (|Member|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.MemberAccess -> Some (exp :?> MemberExpression)
    | _ -> None

let (|Parameter|_|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Parameter -> Some (exp :?> ParameterExpression)
    | _ -> None

let getColumnComparison (expType: ExpressionType, value: obj) =
    match expType with
    | ExpressionType.Equal -> Eq value
    | ExpressionType.NotEqual -> Ne value
    | ExpressionType.GreaterThan -> Gt value
    | ExpressionType.GreaterThanOrEqual -> Ge value
    | ExpressionType.LessThan -> Lt value
    | ExpressionType.LessThanOrEqual -> Le value
    | _ -> failwith "Unsupported statement"

let rec visit (exp: Expression) : Where =
    match exp with
    | Lambda x ->
        visit x.Body

    | Unary x ->
        visit x.Operand

    | Binary x -> 
        match exp.NodeType with
        | ExpressionType.And
        | ExpressionType.AndAlso ->
            let lt = visit x.Left
            let rt = visit x.Right
            Binary (lt, And, rt)
        | ExpressionType.Or
        | ExpressionType.OrElse ->
            let lt = visit x.Left
            let rt = visit x.Right
            Binary (lt, Or, rt)
        | _ ->
            match x.Left, x.Right with
            | Member m, Constant c ->
                let colName = m.Member.Name
                let value = c.Value
                let columnComparison = getColumnComparison(exp.NodeType, value)
                Column (colName, columnComparison)
            | _ ->
                failwith "Not supported"

    | MethodCall x -> 
        failwith "Not Implemented"

    | Constant x -> 
        failwith "Not Implemented"

    | Member x ->
        failwith "Not Implemented"

    | Parameter x -> 
        failwith "Not Implemented"

    | _ ->
        failwithf "Not implemented expression type: %A" exp.NodeType

let visitWhere<'T> (filter: Expression<Func<'T, bool>>) =
    visit (filter :> Expression)

let tbl<'T> = Array.empty<'T>

type SelectExpressionBuilder() =

    let def = 
        { Schema = None
          Table = ""
          Where = Where.Empty
          OrderBy = []
          Pagination = { Skip = 0; Take = None }
          Joins = []
          Aggregates = []
          GroupBy = []
          Distinct = false } : SelectQuery

    member this.For (rows: seq<'T>, f: 'T -> SelectQuery) =
        def

    member __.Yield _ =
        {
            Schema = None
            Table = ""
            Where = Where.Empty
            OrderBy = []
            Pagination = { Skip = 0; Take = None }
            Joins = []
            Aggregates = []
            GroupBy = []
            Distinct = false
        } : SelectQuery

    /// Sets the TABLE name for query
    [<CustomOperation("schema", MaintainsVariableSpace = true)>]
    member __.Schema (state:SelectQuery, name) = { state with Schema = Some name }

    /// Sets the TABLE name for query
    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member __.Table (state:SelectQuery, name) = { state with Table = name }

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:SelectQuery, [<ProjectionParameter>] whereExpression) = 
        let where = visitWhere<'T> whereExpression
        { state with Where = where }

    /// Sets the ORDER BY for multiple columns
    [<CustomOperation("orderByMany", MaintainsVariableSpace = true)>]
    member __.OrderByMany (state:SelectQuery, values) = { state with OrderBy = values }

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:SelectQuery, colName, direction) = { state with OrderBy = [(colName, direction)] }

    /// Sets the SKIP value for query
    [<CustomOperation("skip", MaintainsVariableSpace = true)>]
    member __.Skip (state:SelectQuery, skip) = { state with Pagination = { state.Pagination with Skip = skip } }
    
    /// Sets the TAKE value for query
    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    member __.Take (state:SelectQuery, take) = { state with Pagination = { state.Pagination with Take = Some take } }

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation("skipTake", MaintainsVariableSpace = true)>]
    member __.SkipTake (state:SelectQuery, skip, take) = { state with Pagination = { state.Pagination with Skip = skip; Take = Some take } }

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("innerJoin", MaintainsVariableSpace = true)>]
    member __.InnerJoin (state:SelectQuery, tableName, colName, equalsTo) = { state with Joins = state.Joins @ [InnerJoin(tableName, colName, equalsTo)] }

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true)>]
    member __.LeftJoin (state:SelectQuery, tableName, colName, equalsTo) = { state with Joins = state.Joins @ [LeftJoin(tableName, colName, equalsTo)] }
    
    /// Sets the ORDER BY for multiple columns
    [<CustomOperation("groupByMany", MaintainsVariableSpace = true)>]
    member __.GroupByMany (state:SelectQuery, values) = { state with GroupBy = values }

    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:SelectQuery, colName) = { state with GroupBy = [colName] }

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member __.Count (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Count(colName, alias)] }

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member __.Avg (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Avg(colName, alias)] }
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member __.Sum (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Sum(colName, alias)] }
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member __.Min (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Min(colName, alias)] }
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member __.Max (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Max(colName, alias)] }
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member __.Distinct (state:SelectQuery) = { state with Distinct = true }


let select = SelectExpressionBuilder()
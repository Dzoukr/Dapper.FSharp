module Dapper.FSharp.ExpressionBuilders

open System.Linq.Expressions
open System

let (|Lambda|Unary|Binary|MethodCall|Constant|Member|Parameter|) (exp: Expression) =
    match exp.NodeType with
    | ExpressionType.Lambda -> 
        Lambda (exp :?> LambdaExpression)
    | ExpressionType.ArrayLength
    | ExpressionType.Convert
    | ExpressionType.ConvertChecked
    | ExpressionType.Negate
    | ExpressionType.UnaryPlus
    | ExpressionType.NegateChecked
    | ExpressionType.Not
    | ExpressionType.Quote
    | ExpressionType.TypeAs ->
        Unary (exp :?> UnaryExpression)
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
    | ExpressionType.SubtractChecked -> 
        Binary (exp :?> BinaryExpression)
    | ExpressionType.Call -> 
        MethodCall (exp :?> MethodCallExpression)
    | ExpressionType.Constant ->
        Constant (exp :?> ConstantExpression)
    | ExpressionType.MemberAccess ->
        Member (exp :?> MemberExpression)
    | ExpressionType.Parameter ->
        Parameter (exp :?> ParameterExpression)
    | _ ->
        failwithf "Not implemented expression type: %A" exp.NodeType

let rec visit (exp: Expression) : Where =
    match exp with
    | Lambda x ->
        visit x.Body

    | Unary x ->
        visit x.Operand

    | Binary x -> 
        //match x.NodeType with
        //| ExpressionType.Equal ->
        //    let lt = visit x.Left
        //    let rt = visit x.Right
            
        //let middle : BinaryOperation =
        //    match x.Type with
        //    | ExpressionType.Equal -> 


        
        //let isRightSideNullConstant = 
        //    x.Right.NodeType = (ExpressionType.Constant && (x.Right :?> ConstantExpression).Value = null)

        //match x.NodeType with
        //| ExpressionType.Equal when isRightSideNullConstant -> "IS"
        //| ExpressionType.NotEqual when isRightSideNullConstant -> "IS NOT"
        //| ExpressionType
        failwith "Not Implemented"

    | MethodCall x -> 
        failwith "Not Implemented"

    | Constant x -> 
        failwith "Not Implemented"

    | Member x ->
        failwith "Not Implemented"

    | Parameter x -> 
        failwith "Not Implemented"

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
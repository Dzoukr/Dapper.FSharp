module Dapper.FSharp.ExpressionBuilders

open System.Linq.Expressions
                    
let rec visit (exp: Expression) (where: Where) : Where =
    match exp.NodeType with
    | ExpressionType.Lambda -> 
        let x = exp :?> LambdaExpression
        visit x.Body where

    | ExpressionType.TypeAs ->
        let x = exp :?> UnaryExpression
        visit x.Operand where

    //| ExpressionType.SubtractChecked -> 
    //    let x = exp :?> BinaryExpression
    //    let l = visit x.Left Where.Empty
    //    let r = visit x.Right Where.Empty

    //    let isRightSideNullConstant = 
    //        x.Right.NodeType = (ExpressionType.Constant && (x.Right :?> ConstantExpression).Value = null)

    //    match x.NodeType with
    //    | ExpressionType.Equal when isRightSideNullConstant -> "IS"
    //    | ExpressionType.NotEqual when isRightSideNullConstant -> "IS NOT"
    //    | ExpressionType


    | _ ->
        failwith "Not Implemented"
    

type SelectBuilder() =
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
    [<CustomOperation "schema">]
    member __.Schema (state:SelectQuery, name) = { state with Schema = Some name }

    /// Sets the TABLE name for query
    [<CustomOperation "table">]
    member __.Table (state:SelectQuery, name) = { state with Table = name }

    /// Sets the WHERE condition
    [<CustomOperation "where">]
    member __.Where (state:SelectQuery, where:Where) = { state with Where = where }

    /// Sets the ORDER BY for multiple columns
    [<CustomOperation "orderByMany">]
    member __.OrderByMany (state:SelectQuery, values) = { state with OrderBy = values }

    /// Sets the ORDER BY for single column
    [<CustomOperation "orderBy">]
    member __.OrderBy (state:SelectQuery, colName, direction) = { state with OrderBy = [(colName, direction)] }

    /// Sets the SKIP value for query
    [<CustomOperation "skip">]
    member __.Skip (state:SelectQuery, skip) = { state with Pagination = { state.Pagination with Skip = skip } }
    
    /// Sets the TAKE value for query
    [<CustomOperation "take">]
    member __.Take (state:SelectQuery, take) = { state with Pagination = { state.Pagination with Take = Some take } }

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation "skipTake">]
    member __.SkipTake (state:SelectQuery, skip, take) = { state with Pagination = { state.Pagination with Skip = skip; Take = Some take } }

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation "innerJoin">]
    member __.InnerJoin (state:SelectQuery, tableName, colName, equalsTo) = { state with Joins = state.Joins @ [InnerJoin(tableName, colName, equalsTo)] }

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation "leftJoin">]
    member __.LeftJoin (state:SelectQuery, tableName, colName, equalsTo) = { state with Joins = state.Joins @ [LeftJoin(tableName, colName, equalsTo)] }
    
    /// Sets the ORDER BY for multiple columns
    [<CustomOperation "groupByMany">]
    member __.GroupByMany (state:SelectQuery, values) = { state with GroupBy = values }

    /// Sets the ORDER BY for single column
    [<CustomOperation "groupBy">]
    member __.GroupBy (state:SelectQuery, colName) = { state with GroupBy = [colName] }

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation "count">]
    member __.Count (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Count(colName, alias)] }

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation "avg">]
    member __.Avg (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Avg(colName, alias)] }
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation "sum">]
    member __.Sum (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Sum(colName, alias)] }
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation "min">]
    member __.Min (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Min(colName, alias)] }
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation "max">]
    member __.Max (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Max(colName, alias)] }
    
    /// Sets query to return DISTINCT values
    [<CustomOperation "distinct">]
    member __.Distinct (state:SelectQuery) = { state with Distinct = true }

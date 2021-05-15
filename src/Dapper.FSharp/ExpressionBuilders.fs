﻿module Dapper.FSharp.ExpressionBuilders

open System.Linq.Expressions
open System
open System.Collections.Generic

// Where filters
let isIn<'P> (prop: 'P) (values: 'P list) = true
let isNotIn<'P> (prop: 'P) (values: 'P list) = true
let like<'P> (prop: 'P) (pattern: string) = true

type QuerySource<'T>() =
    let query = 
        { Schema = None
          Table = ""
          Where = Where.Empty
          OrderBy = []
          Pagination = { Skip = 0; Take = None }
          Joins = []
          Aggregates = []
          GroupBy = []
          Distinct = false } : SelectQuery

    interface IEnumerable<'T> with
        member this.GetEnumerator(): Collections.IEnumerator = 
            Seq.empty<'T>.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator(): IEnumerator<'T> = 
            Seq.empty<'T>.GetEnumerator()

    member val Query : SelectQuery = query with get,set

type SelectExpressionBuilder<'T>() =
    let def = new QuerySource<'T>()

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        let t = typeof<'T>
        // Join returns a tuple
        if t.Name.StartsWith "Tuple" then
            let args = t.GetGenericArguments()
            state.Query <- { state.Query with Table = args.[0].Name }
        else
            state.Query <- { state.Query with Table = t.Name }
        state

    member __.Yield _ =
        def

    member __.Zero _ =
        def

    /// Sets the TABLE name for query
    [<CustomOperation("schema", MaintainsVariableSpace = true)>]
    member __.Schema (state:QuerySource<'T>, name) = 
        state.Query <- { state.Query with Schema = Some name }
        state

    /// Sets the TABLE name for query
    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member __.Table (state:QuerySource<'T>, name) = 
        state.Query <- { state.Query with Table = name }
        state

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let where = ExpressionVisitor.visitWhere<'T> whereExpression
        state.Query <- { state.Query with Where = where }
        state

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector propertySelector
        let orderBy = OrderBy (propertyName, Asc)
        state.Query <- { state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }
        state

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member __.OrderByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector propertySelector
        let orderBy = OrderBy (propertyName, Desc)
        state.Query <- { state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }
        state

    /// Sets the SKIP value for query
    [<CustomOperation("skip", MaintainsVariableSpace = true)>]
    member __.Skip (state:QuerySource<'T>, skip) = 
        state.Query <- { state.Query with Pagination = { state.Query.Pagination with Skip = skip } }
        state
    
    /// Sets the TAKE value for query
    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    member __.Take (state:QuerySource<'T>, take) = 
        state.Query <- { state.Query with Pagination = { state.Query.Pagination with Take = Some take } }
        state

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation("skipTake", MaintainsVariableSpace = true)>]
    member __.SkipTake (state:QuerySource<'T>, skip, take) = 
        state.Query <- { state.Query with Pagination = { state.Query.Pagination with Skip = skip; Take = Some take } }
        state

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("join", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.Join (outerSource: QuerySource<'TOuter>, 
                    innerSource: QuerySource<'TInner>, 
                    outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                    innerKeySelector: Expression<Func<'TInner,'Key>>, 
                    resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 
        let outerPropertyName = ExpressionVisitor.visitPropertySelector(outerKeySelector)
        let innerPropertyName = ExpressionVisitor.visitPropertySelector(innerKeySelector)
        let outerTable = typeof<'TOuter>.Name
        let innerTable = typeof<'TInner>.Name
        let join = InnerJoin (innerTable, innerPropertyName, outerPropertyName)
        let result = QuerySource<'Result>()
        result.Query <- { outerSource.Query with Joins = outerSource.Query.Joins @ [join] }
        result

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.LeftJoin (outerSource: QuerySource<'TOuter>, 
                    innerSource: QuerySource<'TInner>, 
                    outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                    innerKeySelector: Expression<Func<'TInner,'Key>>, 
                    resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 
        let outerPropertyName = ExpressionVisitor.visitPropertySelector(outerKeySelector)
        let innerPropertyName = ExpressionVisitor.visitPropertySelector(innerKeySelector)
        let outerTable = typeof<'TOuter>.Name
        let innerTable = typeof<'TInner>.Name
        let join = LeftJoin (innerTable, innerPropertyName, outerPropertyName)
        let result = QuerySource<'Result>()
        result.Query <- { outerSource.Query with Joins = outerSource.Query.Joins @ [join] }
        result


    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with GroupBy = state.Query.GroupBy @ [propertyName] }
        state

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member __.Count (state:QuerySource<'T>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Count(colName, alias)] }
        state

    /// COUNT aggregate function for the selected column
    [<CustomOperation("countBy", MaintainsVariableSpace = true)>]
    member __.CountBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Count(propertyName, propertyName)] }
        state

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member __.Avg (state:QuerySource<'T>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Avg(colName, alias)] }
        state

    /// AVG aggregate function for the selected column
    [<CustomOperation("avgBy", MaintainsVariableSpace = true)>]
    member __.AvgBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Avg(propertyName, propertyName)] }
        state
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member __.Sum (state:QuerySource<'T>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Sum(colName, alias)] }
        state

    /// SUM aggregate function for the selected column
    [<CustomOperation("sumBy", MaintainsVariableSpace = true)>]
    member __.SumBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Sum(propertyName, propertyName)] }
        state
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member __.Min (state:QuerySource<'T>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Min(colName, alias)] }
        state

    /// MIN aggregate function for the selected column
    [<CustomOperation("minBy", MaintainsVariableSpace = true)>]
    member __.MinBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Min(propertyName, propertyName)] }
        state
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member __.Max (state:QuerySource<'T>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Max(colName, alias)] }
        state

    /// MIN aggregate function for the selected column
    [<CustomOperation("maxBy", MaintainsVariableSpace = true)>]
    member __.MaxBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Max(propertyName, propertyName)] }
        state
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member __.Distinct (state:QuerySource<'T>) = 
        state.Query <- { state.Query with Distinct = true }
        state

    member __.Run (state: QuerySource<'T>) =
        state.Query

let select<'T> = SelectExpressionBuilder<'T>()

/// Used in the 'for' statement
let entity<'T> = QuerySource<'T>()

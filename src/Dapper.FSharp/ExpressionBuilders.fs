module Dapper.FSharp.ExpressionBuilders

open System.Linq.Expressions
open System
open System.Collections.Generic

// Where filters
let isIn<'P> (prop: 'P) (values: 'P list) = true
let isNotIn<'P> (prop: 'P) (values: 'P list) = true
let like<'P> (prop: 'P) (pattern: string) = true

type QuerySource<'T, 'Q>() =
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

type SelectExpressionBuilder<'T, 'Q>() =
    let def = new QuerySource<'T, 'Q>()

    interface IEnumerable<'T> with
        member this.GetEnumerator(): Collections.IEnumerator = 
            Seq.empty<'T>.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator(): IEnumerator<'T> = 
            Seq.empty<'T>.GetEnumerator()
                    
    member this.For (items: seq<'T>, f: 'T -> QuerySource<'T, 'Q>) =
        let t = typeof<'T>
        def.Query <- { def.Query with Table = t.Name }
        def

    member __.Yield _ =
        def

    //member __.Zero _ =
    //    def

    /// Sets the TABLE name for query
    [<CustomOperation("schema", MaintainsVariableSpace = true)>]
    member __.Schema (state:QuerySource<'T, 'Q>, name) = 
        state.Query <- { state.Query with Schema = Some name }
        state

    /// Sets the TABLE name for query
    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member __.Table (state:QuerySource<'T, 'Q>, name) = 
        state.Query <- { state.Query with Table = name }
        state

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] whereExpression) = 
        let where = ExpressionVisitor.visitWhere<'T> whereExpression
        state.Query <- { state.Query with Where = where }
        state

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let orderBy = ExpressionVisitor.visitOrderBy<'T, 'Prop>(propertySelector, Asc)
        state.Query <- { state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }
        state

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member __.OrderByDescending (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let orderBy = ExpressionVisitor.visitOrderBy<'T, 'Prop>(propertySelector, Desc)
        state.Query <- { state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }
        state

    /// Sets the SKIP value for query
    [<CustomOperation("skip", MaintainsVariableSpace = true)>]
    member __.Skip (state:QuerySource<'T, 'Q>, skip) = 
        state.Query <- { state.Query with Pagination = { state.Query.Pagination with Skip = skip } }
        state
    
    /// Sets the TAKE value for query
    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    member __.Take (state:QuerySource<'T, 'Q>, take) = 
        state.Query <- { state.Query with Pagination = { state.Query.Pagination with Take = Some take } }
        state

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation("skipTake", MaintainsVariableSpace = true)>]
    member __.SkipTake (state:QuerySource<'T, 'Q>, skip, take) = 
        state.Query <- { state.Query with Pagination = { state.Query.Pagination with Skip = skip; Take = Some take } }
        state

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("join", IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.Join (//state:QuerySource<'T, 'Q>, 
        outerSource: QuerySource<'TOuter, 'Q>, 
        innerSource: QuerySource<'TInner, 'Q>, 
        outerKeySelector: Expression<Func<'TOuter,'Key>>, 
        innerKeySelector: Expression<Func<'TInner,'Key>>, 
        resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 
        //let join = ExpressionVisitor.visitJoin2<'TInner, 'TOuter, SelectQuery, 'TKey>(innerKeySelector, outerKeySelector, resultSelector)
        let newResult = QuerySource<'Result, 'Q>()
        //newResult.Query <- state.Query
        newResult

    //member _.Zero () =
    //    def

    ///// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    //[<CustomOperation("innerJoin", MaintainsVariableSpace = true)>]
    //member __.InnerJoin (state:QuerySource<'T, 'Q>, joinOn) = 
    //    let join = ExpressionVisitor.visitJoin (joinOn, InnerJoin, state.Schema)
    //    { state with Joins = state.Joins @ [join] }

    ///// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    //[<CustomOperation("leftJoin", MaintainsVariableSpace = true)>]
    //member __.LeftJoin (state:QueryWrapper<'T, 'Q>, joinOn) = 
    //    let join = ExpressionVisitor.visitJoin (joinOn, LeftJoin, state.Schema)
    //    { state with Joins = state.Joins @ [join] }
    
    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with GroupBy = state.Query.GroupBy @ [propertyName] }
        state

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member __.Count (state:QuerySource<'T, 'Q>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Count(colName, alias)] }
        state

    /// COUNT aggregate function for the selected column
    [<CustomOperation("countBy", MaintainsVariableSpace = true)>]
    member __.CountBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Count(propertyName, propertyName)] }
        state

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member __.Avg (state:QuerySource<'T, 'Q>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Avg(colName, alias)] }
        state

    /// AVG aggregate function for the selected column
    [<CustomOperation("avgBy", MaintainsVariableSpace = true)>]
    member __.AvgBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Avg(propertyName, propertyName)] }
        state
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member __.Sum (state:QuerySource<'T, 'Q>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Sum(colName, alias)] }
        state

    /// SUM aggregate function for the selected column
    [<CustomOperation("sumBy", MaintainsVariableSpace = true)>]
    member __.SumBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Sum(propertyName, propertyName)] }
        state
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member __.Min (state:QuerySource<'T, 'Q>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Min(colName, alias)] }
        state

    /// MIN aggregate function for the selected column
    [<CustomOperation("minBy", MaintainsVariableSpace = true)>]
    member __.MinBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Min(propertyName, propertyName)] }
        state
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member __.Max (state:QuerySource<'T, 'Q>, colName, alias) = 
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Max(colName, alias)] }
        state

    /// MIN aggregate function for the selected column
    [<CustomOperation("maxBy", MaintainsVariableSpace = true)>]
    member __.MaxBy (state:QuerySource<'T, 'Q>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        state.Query <- { state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Max(propertyName, propertyName)] }
        state
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member __.Distinct (state:QuerySource<'T, 'Q>) = 
        state.Query <- { state.Query with Distinct = true }
        state

    member __.Run (state: QuerySource<'T, 'Q>) =
        state.Query

let select<'T, 'Q> = SelectExpressionBuilder<'T, 'Q>()

/// Used in the 'for' statement
//let entity<'T, 'Q> = QuerySource<'T, 'Q>() //Seq.empty<'Table>
let entity<'T> = QuerySource<'T, _>()
//let entity<'T> = Seq.empty<'T>

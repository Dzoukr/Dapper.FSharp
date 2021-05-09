module Dapper.FSharp.ExpressionBuilders

/// Used in the 'for' statement
let entity<'Table> = Seq.empty<'Table>

// Where filters
let isIn<'P> (prop: 'P) (values: 'P list) = true
let isNotIn<'P> (prop: 'P) (values: 'P list) = true
let like<'P> (prop: 'P) (pattern: string) = true

type SelectExpressionBuilder<'T>() =
    
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

    member this.For (items: seq<'T>, f: 'T -> SelectQuery) =
        let t = typeof<'T>
        { def with Table = t.Name }

    member __.Yield _ =
        def

    /// Sets the TABLE name for query
    [<CustomOperation("schema", MaintainsVariableSpace = true)>]
    member __.Schema (state:SelectQuery, name) = { state with Schema = Some name }

    /// Sets the TABLE name for query
    [<CustomOperation("table", MaintainsVariableSpace = true)>]
    member __.Table (state:SelectQuery, name) = { state with Table = name }

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:SelectQuery, [<ProjectionParameter>] whereExpression) = 
        let where = ExpressionVisitor.visitWhere<'T> whereExpression
        { state with Where = where }

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let orderBy = ExpressionVisitor.visitOrderBy<'T, 'Prop>(propertySelector, Asc)
        { state with OrderBy = state.OrderBy @ [orderBy] }

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member __.OrderByDescending (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let orderBy = ExpressionVisitor.visitOrderBy<'T, 'Prop>(propertySelector, Desc)
        { state with OrderBy = state.OrderBy @ [orderBy] }

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
    member __.InnerJoin (state:SelectQuery, joinOn) = 
        let join = ExpressionVisitor.visitJoin (joinOn, InnerJoin)
        { state with Joins = state.Joins @ [join] }

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true)>]
    member __.LeftJoin (state:SelectQuery, joinOn) = 
        let join = ExpressionVisitor.visitJoin (joinOn, LeftJoin)
        { state with Joins = state.Joins @ [join] }
    
    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        { state with GroupBy = state.GroupBy @ [propertyName] }

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member __.Count (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Count(colName, alias)] }

    /// COUNT aggregate function for the selected column
    [<CustomOperation("countBy", MaintainsVariableSpace = true)>]
    member __.CountBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        { state with Aggregates = state.Aggregates @ [Aggregate.Count(propertyName, propertyName)] }

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member __.Avg (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Avg(colName, alias)] }

    /// AVG aggregate function for the selected column
    [<CustomOperation("avgBy", MaintainsVariableSpace = true)>]
    member __.AvgBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        { state with Aggregates = state.Aggregates @ [Aggregate.Avg(propertyName, propertyName)] }
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member __.Sum (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Sum(colName, alias)] }

    /// SUM aggregate function for the selected column
    [<CustomOperation("sumBy", MaintainsVariableSpace = true)>]
    member __.SumBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        { state with Aggregates = state.Aggregates @ [Aggregate.Sum(propertyName, propertyName)] }
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member __.Min (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Min(colName, alias)] }

    /// MIN aggregate function for the selected column
    [<CustomOperation("minBy", MaintainsVariableSpace = true)>]
    member __.MinBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        { state with Aggregates = state.Aggregates @ [Aggregate.Min(propertyName, propertyName)] }
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member __.Max (state:SelectQuery, colName, alias) = { state with Aggregates = state.Aggregates @ [Aggregate.Max(colName, alias)] }

    /// MIN aggregate function for the selected column
    [<CustomOperation("maxBy", MaintainsVariableSpace = true)>]
    member __.MaxBy (state:SelectQuery, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop>(propertySelector)
        { state with Aggregates = state.Aggregates @ [Aggregate.Max(propertyName, propertyName)] }
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member __.Distinct (state:SelectQuery) = { state with Distinct = true }

let select<'T> = SelectExpressionBuilder<'T>()
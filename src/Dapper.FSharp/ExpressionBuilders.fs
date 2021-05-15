module Dapper.FSharp.ExpressionBuilders

open System.Linq.Expressions
open System
open System.Collections.Generic

// Where filters
let isIn<'P> (prop: 'P) (values: 'P list) = true
let isNotIn<'P> (prop: 'P) (values: 'P list) = true
let like<'P> (prop: 'P) (pattern: string) = true

type TableInfo = {
    Name: string
    Schema: string option
}

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
        member this.GetEnumerator() = Seq.empty<'T>.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator() = Seq.empty<'T>.GetEnumerator()

    member val Query : SelectQuery = query with get,set
    member val Tables : Map<string, TableInfo> = Map.empty with get,set

type SelectExpressionBuilder<'T>() =
    let def = new QuerySource<'T>()

    /// Merges two maps of table info records
    let mergeTables (a: Map<string, TableInfo>, b: Map<string, TableInfo>) =
        Map (Seq.concat [ (Map.toSeq a); (Map.toSeq b) ])

    /// Fully qualifies a column with: {schema}.{table}.{column}
    let fullyQualifyColumn (tables: Map<string, TableInfo>) (entityType: Type, columnName: string) =
        let tbl = tables.[entityType.Name]
        match tbl.Schema with
        | Some schema -> sprintf "%s.%s.%s" schema tbl.Name columnName
        | None -> sprintf "%s.%s" tbl.Name columnName

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        state

    member __.Yield _ =
        def

    member __.Zero _ =
        def

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let where = ExpressionVisitor.visitWhere<'T> whereExpression (fullyQualifyColumn state.Tables)
        state.Query <- { state.Query with Where = where }
        state

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
        let orderBy = OrderBy (propertyName, Asc)
        state.Query <- { state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }
        state

    /// Sets the ORDER BY for single column
    [<CustomOperation("thenBy", MaintainsVariableSpace = true)>]
    member __.ThenBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        __.OrderBy (state, propertySelector)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member __.OrderByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
        let orderBy = OrderBy (propertyName, Desc)
        state.Query <- { state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }
        state

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("thenByDescending", MaintainsVariableSpace = true)>]
    member __.ThenByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        __.OrderByDescending (state, propertySelector)

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

        let mergedTables = mergeTables (outerSource.Tables, innerSource.Tables)
        let outerPropertyName = ExpressionVisitor.visitPropertySelector<'TOuter, 'Key> outerKeySelector (fullyQualifyColumn mergedTables)
        
        // Do not qualify inner column name because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}"
        let doNotQualifyColumn (entityType: Type, columnName: string) = columnName
        let innerPropertyName = ExpressionVisitor.visitPropertySelector<'TInner, 'Key> innerKeySelector doNotQualifyColumn

        let innerTableName = 
            let tbl = mergedTables.[typeof<'TInner>.Name]
            match tbl.Schema with
            | Some schema -> sprintf "%s.%s" schema tbl.Name
            | None -> tbl.Name
        let join = InnerJoin (innerTableName, innerPropertyName, outerPropertyName)
        let result = QuerySource<'Result>()
        result.Tables <- mergedTables
        result.Query <- { outerSource.Query with Joins = outerSource.Query.Joins @ [join] }
        result

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.LeftJoin (outerSource: QuerySource<'TOuter>, 
                        innerSource: QuerySource<'TInner>, 
                        outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                        innerKeySelector: Expression<Func<'TInner,'Key>>, 
                        resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTables (outerSource.Tables, innerSource.Tables)
        let outerPropertyName = ExpressionVisitor.visitPropertySelector<'TOuter, 'Key> outerKeySelector (fullyQualifyColumn mergedTables)
        
        // Do not qualify inner column name because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}"
        let doNotQualifyColumn (entityType: Type, columnName: string) = columnName
        let innerPropertyName = ExpressionVisitor.visitPropertySelector<'TInner, 'Key> innerKeySelector doNotQualifyColumn

        let innerTableName = 
            let tbl = mergedTables.[typeof<'TInner>.Name]
            match tbl.Schema with
            | Some schema -> sprintf "%s.%s" schema tbl.Name
            | None -> tbl.Name
        let join = LeftJoin (innerTableName, innerPropertyName, outerPropertyName)
        let result = QuerySource<'Result>()
        result.Tables <- mergedTables
        result.Query <- { outerSource.Query with Joins = outerSource.Query.Joins @ [join] }

        result

    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
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
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
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
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
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
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
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
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
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
        let propertyName = ExpressionVisitor.visitPropertySelector<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
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

/// Maps the entity 'T to a table of the same name.
let entity<'T> = 
    let qs = QuerySource<'T>()
    let entityName = typeof<'T>.Name
    qs.Query <- { qs.Query with Table = entityName }
    qs.Tables <- qs.Tables.Add(entityName, { Name = entityName; Schema = None })
    qs

/// Maps the entity 'T to a table of the given name.
let mapTable<'T> (tableName: string) (qs: QuerySource<'T>) = 
    qs.Query <- { qs.Query with Table = tableName }
    let entityName = typeof<'T>.Name
    let tbl = qs.Tables.[entityName]
    qs.Tables <- qs.Tables.Add(entityName, { tbl with Name = tableName })
    qs

/// Maps the entity 'T to a schema of the given name.
let mapSchema<'T> (schemaName: string) (qs: QuerySource<'T>) =
    qs.Query <- { qs.Query with Schema = Some schemaName }
    let entityName = typeof<'T>.Name
    let tbl = qs.Tables.[entityName]
    qs.Tables <- qs.Tables.Add(entityName, { tbl with Schema = Some schemaName })
    qs
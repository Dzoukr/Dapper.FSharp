/// LINQ query builders
module Dapper.FSharp.LinqBuilders

open System.Linq.Expressions
open System
open System.Collections.Generic

type TableInfo = {
    Name: string
    Schema: string option
}

let private defQuery = 
    { Schema = None
      Table = ""
      Where = Where.Empty
      OrderBy = []
      Pagination = { Skip = 0; Take = None }
      Joins = []
      Aggregates = []
      GroupBy = []
      Distinct = false } : SelectQuery

[<AutoOpen>]
module FQ = 
    /// Fully qualified entity type name
    type FQName = private FQName of string
    let fqName (t: Type) = FQName t.FullName

type QuerySource<'T>(query, tables) =
    interface IEnumerable<'T> with
        member this.GetEnumerator() = Seq.empty<'T>.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator() = Seq.empty<'T>.GetEnumerator()

    member val Query : SelectQuery = query
    member val Tables : Map<FQName, TableInfo> = tables

type SelectExpressionBuilder<'T>() =
    let def = new QuerySource<'T>(defQuery, Map.empty)

    /// Merges two maps of table info records
    let mergeTables (a: Map<FQName, TableInfo>, b: Map<FQName, TableInfo>) =
        Map (Seq.concat [ (Map.toSeq a); (Map.toSeq b) ])

    /// Fully qualifies a column with: {schema}.{table}.{column}
    let fullyQualifyColumn (tables: Map<FQName, TableInfo>) (property: Reflection.MemberInfo) =
        let tbl = tables.[fqName property.DeclaringType]
        match tbl.Schema with
        | Some schema -> sprintf "%s.%s.%s" schema tbl.Name property.Name
        | None -> sprintf "%s.%s" tbl.Name property.Name

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        state

    member __.Yield _ =
        def

    member __.Zero _ =
        def

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let where = LinqExpressionVisitors.visitWhere<'T> whereExpression (fullyQualifyColumn state.Tables)
        QuerySource<'T>({ state.Query with Where = where }, state.Tables)

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        let orderBy = OrderBy (propertyName, Asc)
        QuerySource<'T>({ state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }, state.Tables)

    /// Sets the ORDER BY for single column
    [<CustomOperation("thenBy", MaintainsVariableSpace = true)>]
    member __.ThenBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        let orderBy = OrderBy (propertyName, Asc)
        QuerySource<'T>({ state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }, state.Tables)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member __.OrderByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        let orderBy = OrderBy (propertyName, Desc)
        QuerySource<'T>({ state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }, state.Tables)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("thenByDescending", MaintainsVariableSpace = true)>]
    member __.ThenByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        let orderBy = OrderBy (propertyName, Desc)
        QuerySource<'T>({ state.Query with OrderBy = state.Query.OrderBy @ [orderBy] }, state.Tables)

    /// Sets the SKIP value for query
    [<CustomOperation("skip", MaintainsVariableSpace = true)>]
    member __.Skip (state:QuerySource<'T>, skip) = 
        QuerySource<'T>({ state.Query with Pagination = { state.Query.Pagination with Skip = skip } }, state.Tables)
    
    /// Sets the TAKE value for query
    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    member __.Take (state:QuerySource<'T>, take) = 
        QuerySource<'T>({ state.Query with Pagination = { state.Query.Pagination with Take = Some take } }, state.Tables)

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation("skipTake", MaintainsVariableSpace = true)>]
    member __.SkipTake (state:QuerySource<'T>, skip, take) = 
        QuerySource<'T>({ state.Query with Pagination = { state.Query.Pagination with Skip = skip; Take = Some take } }, state.Tables)

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("join", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.Join (outerSource: QuerySource<'TOuter>, 
                    innerSource: QuerySource<'TInner>, 
                    outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                    innerKeySelector: Expression<Func<'TInner,'Key>>, 
                    resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTables (outerSource.Tables, innerSource.Tables)
        let outerPropertyName = LinqExpressionVisitors.visitPropertySelector<'TOuter, 'Key> outerKeySelector |> fullyQualifyColumn mergedTables
        
        // Do not qualify inner column name because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}"
        let innerProperty = LinqExpressionVisitors.visitPropertySelector<'TInner, 'Key> innerKeySelector

        let innerTableName = 
            let tbl = mergedTables.[fqName innerProperty.DeclaringType]
            match tbl.Schema with
            | Some schema -> sprintf "%s.%s" schema tbl.Name
            | None -> tbl.Name
        let join = InnerJoin (innerTableName, innerProperty.Name, outerPropertyName)
        QuerySource<'Result>({ outerSource.Query with Joins = outerSource.Query.Joins @ [join] }, mergedTables)

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.LeftJoin (outerSource: QuerySource<'TOuter>, 
                        innerSource: QuerySource<'TInner>, 
                        outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                        innerKeySelector: Expression<Func<'TInner,'Key>>, 
                        resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTables (outerSource.Tables, innerSource.Tables)
        let outerPropertyName = LinqExpressionVisitors.visitPropertySelector<'TOuter, 'Key> outerKeySelector |> fullyQualifyColumn mergedTables
        
        // Do not qualify inner column name because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}"
        let innerProperty = LinqExpressionVisitors.visitPropertySelector<'TInner, 'Key> innerKeySelector

        let innerTableName = 
            let tbl = mergedTables.[fqName innerProperty.DeclaringType]
            match tbl.Schema with
            | Some schema -> sprintf "%s.%s" schema tbl.Name
            | None -> tbl.Name
        let join = LeftJoin (innerTableName, innerProperty.Name, outerPropertyName)
        QuerySource<'Result>({ outerSource.Query with Joins = outerSource.Query.Joins @ [join] }, mergedTables)

    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let properties = LinqExpressionVisitors.visitGroupBy<'T, 'Prop> propertySelector (fullyQualifyColumn state.Tables)
        QuerySource<'T>({ state.Query with GroupBy = state.Query.GroupBy @ properties}, state.Tables)

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member __.Count (state:QuerySource<'T>, colName, alias) = 
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Count(colName, alias)] }, state.Tables)

    /// COUNT aggregate function for the selected column
    [<CustomOperation("countBy", MaintainsVariableSpace = true)>]
    member __.CountBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Count(propertyName, propertyName)] }, state.Tables)

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member __.Avg (state:QuerySource<'T>, colName, alias) = 
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Avg(colName, alias)] }, state.Tables)

    /// AVG aggregate function for the selected column
    [<CustomOperation("avgBy", MaintainsVariableSpace = true)>]
    member __.AvgBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Avg(propertyName, propertyName)] }, state.Tables)
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member __.Sum (state:QuerySource<'T>, colName, alias) = 
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Sum(colName, alias)] }, state.Tables)

    /// SUM aggregate function for the selected column
    [<CustomOperation("sumBy", MaintainsVariableSpace = true)>]
    member __.SumBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Sum(propertyName, propertyName)] }, state.Tables)
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member __.Min (state:QuerySource<'T>, colName, alias) = 
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Min(colName, alias)] }, state.Tables)

    /// MIN aggregate function for the selected column
    [<CustomOperation("minBy", MaintainsVariableSpace = true)>]
    member __.MinBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Min(propertyName, propertyName)] }, state.Tables)
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member __.Max (state:QuerySource<'T>, colName, alias) = 
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Max(colName, alias)] }, state.Tables)

    /// MIN aggregate function for the selected column
    [<CustomOperation("maxBy", MaintainsVariableSpace = true)>]
    member __.MaxBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.Tables
        QuerySource<'T>({ state.Query with Aggregates = state.Query.Aggregates @ [Aggregate.Max(propertyName, propertyName)] }, state.Tables)
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member __.Distinct (state:QuerySource<'T>) = 
        QuerySource<'T>({ state.Query with Distinct = true }, state.Tables)

    /// Selects all (needed only when there are no other clauses "for" or "join").
    [<CustomOperation("selectAll", MaintainsVariableSpace = true)>]
    member __.Select (state:QuerySource<'T>) = 
        state

    member __.Run (state: QuerySource<'T>) =
        state.Query

let select<'T> = SelectExpressionBuilder<'T>()

/// Maps the entity 'T to a table of the same name.
let entity<'T> = 
    let ent = typeof<'T>
    let tables = Map [fqName ent, { Name = ent.Name; Schema = None }]
    QuerySource<'T>({ defQuery with Table = ent.Name }, tables)

/// Maps the entity 'T to a table of the given name.
let mapTable<'T> (tableName: string) (qs: QuerySource<'T>) = 
    let ent = typeof<'T>
    let fqn = fqName ent
    let tbl = qs.Tables.[fqn]
    let tables = qs.Tables.Add(fqn, { tbl with Name = tableName })
    QuerySource<'T>({ qs.Query with Table = tableName }, tables)

/// Maps the entity 'T to a schema of the given name.
let mapSchema<'T> (schemaName: string) (qs: QuerySource<'T>) =
    let ent = typeof<'T>
    let fqn = fqName ent
    let tbl = qs.Tables.[fqn]
    let tables = qs.Tables.Add(fqn, { tbl with Schema = Some schemaName })
    QuerySource<'T>({ qs.Query with Schema = Some schemaName }, tables)

/// WHERE column is IN values
let isIn<'P> (prop: 'P) (values: 'P list) = true
/// WHERE column is NOT IN values
let isNotIn<'P> (prop: 'P) (values: 'P list) = true
/// WHERE column like value   
let like<'P> (prop: 'P) (pattern: string) = true
/// WHERE column not like value   
let notLike<'P> (prop: 'P) (pattern: string) = true
/// WHERE column IS NULL
let isNullValue<'P> (prop: 'P) = true
/// WHERE column IS NOT NULL
let isNotNullValue<'P> (prop: 'P) = true
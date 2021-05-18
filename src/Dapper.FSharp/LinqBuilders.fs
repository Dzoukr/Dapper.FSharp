/// LINQ query builders
module Dapper.FSharp.LinqBuilders

open System.Linq.Expressions
open System
open System.Collections.Generic

[<AutoOpen>]
module FQ = 
    /// Fully qualified entity type name
    type [<Struct>] FQName = private FQName of string
    let fqName (t: Type) = FQName t.FullName

type TableMapping = { Name: string; Schema: string option }

/// Fully qualifies a column with: {?schema}.{table}.{column}
let private fullyQualifyColumn (tables: Map<FQName, TableMapping>) (property: Reflection.MemberInfo) =
    let tbl = tables.[fqName property.DeclaringType]
    match tbl.Schema with
    | Some schema -> sprintf "%s.%s.%s" schema tbl.Name property.Name
    | None -> sprintf "%s.%s" tbl.Name property.Name

type QuerySource<'T>(tableMappings) =
    interface IEnumerable<'T> with
        member this.GetEnumerator() = Seq.empty<'T>.GetEnumerator() :> Collections.IEnumerator
        member this.GetEnumerator() = Seq.empty<'T>.GetEnumerator()
    
    member val TableMappings : Map<FQName, TableMapping> = tableMappings
    
    member __.GetOuterTableMapping() = 
        let outerEntity = typeof<'T>
        let fqn = 
            if outerEntity.Name.StartsWith "Tuple" // True for joined tables
            then outerEntity.GetGenericArguments() |> Array.head |> fqName
            else outerEntity |> fqName
        __.TableMappings.[fqn]

type SelectQuerySource<'T>(query, tableMappings) = 
    inherit QuerySource<'T>(tableMappings)
    member val Query : SelectQuery = query

type DeleteQuerySource<'T>(query, tableMappings) =
    inherit QuerySource<'T>(tableMappings)
    member val Query : DeleteQuery = query

/// Maps the entity 'T to a table of the same name.
let entity<'T> = 
    let ent = typeof<'T>
    let tables = Map [fqName ent, { Name = ent.Name; Schema = None }]
    QuerySource<'T>(tables)

/// Maps the entity 'T to a table of the given name.
let mapTable<'T> (tableName: string) (qs: QuerySource<'T>) = 
    let ent = typeof<'T>
    let fqn = fqName ent
    let tbl = qs.TableMappings.[fqn]
    let tables = qs.TableMappings.Add(fqn, { tbl with Name = tableName })
    QuerySource<'T>(tables)

/// Maps the entity 'T to a schema of the given name.
let mapSchema<'T> (schemaName: string) (qs: QuerySource<'T>) =
    let ent = typeof<'T>
    let fqn = fqName ent
    let tbl = qs.TableMappings.[fqn]
    let tables = qs.TableMappings.Add(fqn, { tbl with Schema = Some schemaName })
    QuerySource<'T>(tables)

type SelectExpressionBuilder<'T>() =

    let getQueryOrDefault (state: QuerySource<'Result>) =
        match state with
        | :? SelectQuerySource<'Result> as sqs -> sqs.Query
        | _ -> 
            { Schema = None
              Table = ""
              Where = Where.Empty
              OrderBy = []
              Pagination = { Skip = 0; Take = None }
              Joins = []
              Aggregates = []
              GroupBy = []
              Distinct = false } : SelectQuery    

    let mergeTableMappings (a: Map<FQName, TableMapping>, b: Map<FQName, TableMapping>) =
        Map (Seq.concat [ (Map.toSeq a); (Map.toSeq b) ])

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        let tbl = state.GetOuterTableMapping()
        let query = state |> getQueryOrDefault
        SelectQuerySource({ query with Table = tbl.Name; Schema = tbl.Schema }, state.TableMappings)

    member __.Yield _ =
        QuerySource<'T>(Map.empty)

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let query = state |> getQueryOrDefault
        let where = LinqExpressionVisitors.visitWhere<'T> whereExpression (fullyQualifyColumn state.TableMappings)
        SelectQuerySource<'T>({ query with Where = where }, state.TableMappings)

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member __.OrderBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Asc)
        SelectQuerySource<'T>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the ORDER BY for single column
    [<CustomOperation("thenBy", MaintainsVariableSpace = true)>]
    member __.ThenBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Asc)
        SelectQuerySource<'T>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member __.OrderByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Desc)
        SelectQuerySource<'T>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("thenByDescending", MaintainsVariableSpace = true)>]
    member __.ThenByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Desc)
        SelectQuerySource<'T>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the SKIP value for query
    [<CustomOperation("skip", MaintainsVariableSpace = true)>]
    member __.Skip (state:QuerySource<'T>, skip) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Pagination = { query.Pagination with Skip = skip } }, state.TableMappings)
    
    /// Sets the TAKE value for query
    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    member __.Take (state:QuerySource<'T>, take) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Pagination = { query.Pagination with Take = Some take } }, state.TableMappings)

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation("skipTake", MaintainsVariableSpace = true)>]
    member __.SkipTake (state:QuerySource<'T>, skip, take) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Pagination = { query.Pagination with Skip = skip; Take = Some take } }, state.TableMappings)

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("join", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.Join (outerSource: QuerySource<'TOuter>, 
                    innerSource: QuerySource<'TInner>, 
                    outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                    innerKeySelector: Expression<Func<'TInner,'Key>>, 
                    resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTableMappings (outerSource.TableMappings, innerSource.TableMappings)
        let outerPropertyName = LinqExpressionVisitors.visitPropertySelector<'TOuter, 'Key> outerKeySelector |> fullyQualifyColumn mergedTables
        
        // Do not fully qualify inner column name because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}"
        let innerProperty = LinqExpressionVisitors.visitPropertySelector<'TInner, 'Key> innerKeySelector
        let innerTableName = 
            let tbl = mergedTables.[fqName innerProperty.DeclaringType]
            match tbl.Schema with
            | Some schema -> sprintf "%s.%s" schema tbl.Name
            | None -> tbl.Name

        let join = InnerJoin (innerTableName, innerProperty.Name, outerPropertyName)
        let outerQuery = outerSource |> getQueryOrDefault
        SelectQuerySource<'Result>({ outerQuery with Joins = outerQuery.Joins @ [join] }, mergedTables)

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member __.LeftJoin (outerSource: QuerySource<'TOuter>, 
                        innerSource: QuerySource<'TInner>, 
                        outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                        innerKeySelector: Expression<Func<'TInner,'Key>>, 
                        resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTableMappings (outerSource.TableMappings, innerSource.TableMappings)
        let outerPropertyName = LinqExpressionVisitors.visitPropertySelector<'TOuter, 'Key> outerKeySelector |> fullyQualifyColumn mergedTables
        
        // Do not fully qualify inner column name because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}"
        let innerProperty = LinqExpressionVisitors.visitPropertySelector<'TInner, 'Key> innerKeySelector
        let innerTableName = 
            let tbl = mergedTables.[fqName innerProperty.DeclaringType]
            match tbl.Schema with
            | Some schema -> sprintf "%s.%s" schema tbl.Name
            | None -> tbl.Name

        let join = LeftJoin (innerTableName, innerProperty.Name, outerPropertyName)
        let outerQuery = outerSource |> getQueryOrDefault
        SelectQuerySource<'Result>({ outerQuery with Joins = outerQuery.Joins @ [join] }, mergedTables)

    /// Sets the ORDER BY for single column
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member __.GroupBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let properties = LinqExpressionVisitors.visitGroupBy<'T, 'Prop> propertySelector (fullyQualifyColumn state.TableMappings)
        SelectQuerySource<'T>({ query with GroupBy = query.GroupBy @ properties}, state.TableMappings)

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member __.Count (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Count(colName, alias)] }, state.TableMappings)

    /// COUNT aggregate function for the selected column
    [<CustomOperation("countBy", MaintainsVariableSpace = true)>]
    member __.CountBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Count(propertyName, propertyName)] }, state.TableMappings)

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member __.Avg (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Avg(colName, alias)] }, state.TableMappings)

    /// AVG aggregate function for the selected column
    [<CustomOperation("avgBy", MaintainsVariableSpace = true)>]
    member __.AvgBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Avg(propertyName, propertyName)] }, state.TableMappings)
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member __.Sum (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Sum(colName, alias)] }, state.TableMappings)

    /// SUM aggregate function for the selected column
    [<CustomOperation("sumBy", MaintainsVariableSpace = true)>]
    member __.SumBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Sum(propertyName, propertyName)] }, state.TableMappings)
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member __.Min (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Min(colName, alias)] }, state.TableMappings)

    /// MIN aggregate function for the selected column
    [<CustomOperation("minBy", MaintainsVariableSpace = true)>]
    member __.MinBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Min(propertyName, propertyName)] }, state.TableMappings)
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member __.Max (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Max(colName, alias)] }, state.TableMappings)

    /// MIN aggregate function for the selected column
    [<CustomOperation("maxBy", MaintainsVariableSpace = true)>]
    member __.MaxBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        SelectQuerySource<'T>({ query with Aggregates = query.Aggregates @ [Aggregate.Max(propertyName, propertyName)] }, state.TableMappings)
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member __.Distinct (state:QuerySource<'T>) = 
        let query = state |> getQueryOrDefault
        SelectQuerySource<'T>({ query with Distinct = true }, state.TableMappings)

    /// Selects all (needed only when there are no other clauses after "for" or "join")
    [<CustomOperation("selectAll", MaintainsVariableSpace = true)>]
    member __.SelectAll (state:QuerySource<'T>) = 
        state :?> SelectQuerySource<'T>

    /// Unwraps the query
    member __.Run (state: QuerySource<'T>) =
        state |> getQueryOrDefault

type DeleteExpressionBuilder<'T>() =

    let getQueryOrDefault (state: QuerySource<'Result>) =
        match state with
        | :? DeleteQuerySource<'Result> as dqs -> dqs.Query
        | _ -> 
            { Schema = None
              Table = ""
              Where = Where.Empty } : DeleteQuery

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        let tbl = state.GetOuterTableMapping()
        let query = state |> getQueryOrDefault
        DeleteQuerySource({ query with Table = tbl.Name; Schema = tbl.Schema }, state.TableMappings)

    member __.Yield _ =
        QuerySource<'T>(Map.empty)

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member __.Where (state:QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let query = state |> getQueryOrDefault
        let where = LinqExpressionVisitors.visitWhere<'T> whereExpression (fullyQualifyColumn state.TableMappings)
        DeleteQuerySource<'T>({ query with Where = where }, state.TableMappings)

    /// Unwraps the query
    member __.Run (state: QuerySource<'T>) =
        state |> getQueryOrDefault

let select<'T> = SelectExpressionBuilder<'T>()
let delete<'T> = DeleteExpressionBuilder<'T>()

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
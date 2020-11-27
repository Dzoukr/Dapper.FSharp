[<AutoOpen>]
module Dapper.FSharp.Builders

type InsertBuilder<'a>() =
    member __.Yield _ =
        {
            Schema = None
            Table = ""
            Values = []
        } : InsertQuery<'a>

    /// Sets the SCHEMA
    [<CustomOperation "schema">]
    member __.Schema (state:InsertQuery<_>, name) = { state with Schema = Some name }
    
    /// Sets the TABLE name for query
    [<CustomOperation "table">]
    member __.Table (state:InsertQuery<_>, name) = { state with Table = name }

    /// Sets the list of values for INSERT
    [<CustomOperation "values">]
    member __.Values (state:InsertQuery<'a>, values:'a list) = { state with Values = values }

    /// Sets the single value for INSERT
    [<CustomOperation "value">]
    member __.Value (state:InsertQuery<'a>, value:'a) = { state with Values = [value] }

type DeleteBuilder() =
    member __.Yield _ =
        {
            Schema = None
            Table = ""
            Where = Where.Empty
        } : DeleteQuery

    /// Sets the SCHEMA
    [<CustomOperation "schema">]
    member __.Schema (state:DeleteQuery, name) = { state with Schema = Some name }
    
    /// Sets the TABLE name for query
    [<CustomOperation "table">]
    member __.Table (state:DeleteQuery, name) = { state with Table = name }

    /// Sets the WHERE condition
    [<CustomOperation "where">]
    member __.Where (state:DeleteQuery, where:Where) = { state with Where = where }

type UpdateBuilder<'a>() =
    member __.Yield _ =
        {
            Schema = None
            Table = ""
            Value = Unchecked.defaultof<'a>
            Where = Where.Empty
        } : UpdateQuery<'a>

    /// Sets the SCHEMA
    [<CustomOperation "schema">]
    member __.Schema (state:UpdateQuery<_>, name) = { state with Schema = Some name }
    
    /// Sets the TABLE name for query
    [<CustomOperation "table">]
    member __.Table (state:UpdateQuery<_>, name) = { state with Table = name }

    /// Sets the SET of value to UPDATE
    [<CustomOperation "set">]
    member __.Set (state:UpdateQuery<'a>, value:'a) = { state with Value = value }

    /// Sets the WHERE condition
    [<CustomOperation "where">]
    member __.Where (state:UpdateQuery<_>, where:Where) = { state with Where = where }

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
    
let insert<'a> = InsertBuilder<'a>()
let delete = DeleteBuilder()
let update<'a> = UpdateBuilder<'a>()
let select = SelectBuilder()

/// Creates WHERE condition for column
let column name whereComp = Where.Column(name, whereComp)
/// WHERE column value equals to
let eq name (o:obj) = column name (Eq o)
/// WHERE column value not equals to
let ne name (o:obj) = column name (Ne o)
/// WHERE column value greater than
let gt name (o:obj) = column name (Gt o)
/// WHERE column value lower than
let lt name (o:obj) = column name (Lt o)
/// WHERE column value greater/equals than
let ge name (o:obj) = column name (Ge o)
/// WHERE column value lower/equals than
let le name (o:obj) = column name (Le o)
/// WHERE column like value   
let like name (str:string) = column name (Like str)
/// WHERE column not like value   
let notLike name (str:string) = column name (NotLike str)
/// WHERE column is IN values
let isIn name (os:obj list) = column name (In os)
/// WHERE column is NOT IN values
let isNotIn name (os:obj list) = column name (NotIn os)   
/// WHERE column IS NULL
let isNullValue name = column name IsNull
/// WHERE column IS NOT NULL
let isNotNullValue name = column name IsNotNull
[<AutoOpen>]
module Dapper.FSharp.Builders

type InsertBuilder<'a>() =
    member __.Yield _ =
        {
            Table = ""
            Values = []
        } : InsertQuery<'a>
    
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
            Table = ""
            Where = Where.Empty
        } : DeleteQuery
    
    /// Sets the TABLE name for query              
    [<CustomOperation "table">]
    member __.Table (state:DeleteQuery, name) = { state with Table = name }        
    
    /// Sets the WHERE condition          
    [<CustomOperation "where">]
    member __.Where (state:DeleteQuery, where:Where) = { state with Where = where }

type UpdateBuilder<'a>() =
    member __.Yield _ =
        {
            Table = ""
            Value = Unchecked.defaultof<'a>
            Where = Where.Empty
        } : UpdateQuery<'a>
                
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
            Table = ""
            Where = Where.Empty
            OrderBy = []
            Pagination = Skip 0
            Joins = []
        } : SelectQuery
    
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
    member __.Skip (state:SelectQuery, skip) = { state with Pagination = Pagination.Skip skip }        
    
    /// Sets the SKIP and TAKE value for query
    [<CustomOperation "skipTake">]
    member __.SkipTake (state:SelectQuery, skip, take) = { state with Pagination = Pagination.SkipTake(skip, take) }
    
    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation "innerJoin">]
    member __.InnerJoin (state:SelectQuery, tableName, colName, equalsTo) = { state with Joins = state.Joins @ [InnerJoin(tableName, colName, equalsTo)] }        
    
    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation "leftJoin">]
    member __.LeftJoin (state:SelectQuery, tableName, colName, equalsTo) = { state with Joins = state.Joins @ [LeftJoin(tableName, colName, equalsTo)] }        
        
let insert<'a> = InsertBuilder<'a>()
let delete = DeleteBuilder()
let update<'a> = UpdateBuilder<'a>()
let select = SelectBuilder()

// helper functions
let column name whereComp = Where.Column(name, whereComp)
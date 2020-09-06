module Dapper.FSharp.MSSQL

open System.Text
open Dapper.FSharp

let private inBrackets (s:string) =
    s.Split('.')
    |> Array.map (sprintf "[%s]")
    |> String.concat "."

module private Evaluators =

    let evalBinary = function
        | And -> "AND"
        | Or -> "OR"

    let evalOrderDirection = function
        | Asc -> "ASC"
        | Desc -> "DESC"

    let rec evalWhere (meta:WhereAnalyzer.FieldWhereMetadata list) (w:Where) =
        match w with
        | Empty -> ""
        | Column (field, comp) ->
            let fieldMeta = meta |> List.find (fun x -> x.Key = (field,comp))
            let withField op = sprintf "%s %s @%s" (inBrackets fieldMeta.Name) op fieldMeta.ParameterName
            match comp with
            | Eq _ -> withField "="
            | Ne _ -> withField "<>"
            | Gt _ -> withField ">"
            | Lt _ -> withField "<"
            | Ge _ -> withField ">="
            | Le _ -> withField "<="
            | In _ -> withField "IN"
            | NotIn _ -> withField "NOT IN"
            | Like _ -> withField "LIKE"
            | IsNull -> sprintf "%s IS NULL" fieldMeta.Name
            | IsNotNull -> sprintf "%s IS NOT NULL" fieldMeta.Name
        | Binary(w1, comb, w2) ->
            match evalWhere meta w1, evalWhere meta w2 with
            | "", fq | fq , "" -> fq
            | fq1, fq2 -> sprintf "(%s %s %s)" fq1 (evalBinary comb) fq2
        | Unary (Not, w) ->
            match evalWhere meta w with
            | "" -> ""
            | v -> sprintf "NOT (%s)" v

    let evalOrderBy (xs:OrderBy list) =
        xs
        |> List.map (fun (n,s) -> sprintf "%s %s" n (evalOrderDirection s))
        |> String.concat ", "

    let evalPagination (pag:Pagination) =
        match pag with
        | { Take = None; Skip = x } when x <= 0 -> ""
        | { Take = None; Skip = o } -> sprintf "OFFSET %i ROWS" o
        | { Take = Some f; Skip = o } -> sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" o f
    
    let evalJoins (joins:Join list) =
        let sb = StringBuilder()
        let evalJoin = function
            | InnerJoin(table,colName,equalsTo) -> sprintf " INNER JOIN %s ON %s.%s=%s" (inBrackets table) (inBrackets table) (inBrackets colName) (inBrackets equalsTo)
            | LeftJoin(table,colName,equalsTo) -> sprintf " LEFT JOIN %s ON %s.%s=%s" (inBrackets table) (inBrackets table) (inBrackets colName) (inBrackets equalsTo)
        joins |> List.map evalJoin |> List.iter (sb.Append >> ignore)
        sb.ToString()
    
    let evalSelectQuery fields meta (q:SelectQuery) =
        let fieldNames = fields |> List.map inBrackets |> String.concat ", "
        // basic query
        let sb = StringBuilder(sprintf "SELECT %s FROM %s" fieldNames (inBrackets q.Table))
        // joins
        let joins = evalJoins q.Joins
        if joins.Length > 0 then sb.Append joins |> ignore
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        // order by
        let orderBy = evalOrderBy q.OrderBy
        if orderBy.Length > 0 then sb.Append (sprintf " ORDER BY %s" orderBy) |> ignore
        // pagination
        let pagination = evalPagination q.Pagination
        if pagination.Length > 0 then sb.Append (sprintf " %s" pagination) |> ignore
        sb.ToString()
    
    let evalInsertQuery fields outputFields (q:InsertQuery<_>) =
        let fieldNames = fields |> List.map inBrackets |> String.concat ", " |> sprintf "(%s)"
        let values =
            q.Values
            |> List.mapi (fun i _ -> fields |> List.map (fun field -> sprintf "@%s%i" field i ) |> String.concat ", " |> sprintf "(%s)")
            |> String.concat ", "
        match outputFields with
        | [] ->
            sprintf "INSERT INTO %s %s VALUES %s" (inBrackets q.Table) fieldNames values
        | outputFields ->
            let outputFieldNames = outputFields |> List.map (sprintf "INSERTED.%s") |> String.concat ", "
            sprintf "INSERT INTO %s %s OUTPUT %s VALUES %s" (inBrackets q.Table) fieldNames outputFieldNames values

    let evalUpdateQuery fields outputFields meta (q:UpdateQuery<'a>) =
        // basic query
        let pairs = fields |> List.map (fun x -> sprintf "%s=@%s" (inBrackets x) x) |> String.concat ", "
        let baseQuery =
            match outputFields with
            | [] ->
                sprintf "UPDATE %s SET %s" (inBrackets q.Table) pairs
            | outputFields ->
                let outputFieldNames = outputFields |> List.map (sprintf "INSERTED.%s") |> String.concat ", "
                sprintf "UPDATE %s SET %s OUTPUT %s" (inBrackets q.Table) pairs outputFieldNames
        let sb = StringBuilder(baseQuery)
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        sb.ToString()

    let evalDeleteQuery outputFields meta (q:DeleteQuery) =
        let baseQuery =
            match outputFields with
            | [] ->
                sprintf "DELETE FROM %s" (inBrackets q.Table)
            | outputFields ->
                let outputFieldNames = outputFields |> List.map (sprintf "DELETED.%s") |> String.concat ", "
                sprintf "DELETE FROM %s OUTPUT %s" (inBrackets q.Table) outputFieldNames
        // basic query
        let sb = StringBuilder(baseQuery)
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        sb.ToString()

open System
open System.Data
open Dapper

let private extractFieldsAndSplit<'a> (j:Join) =
    let table = j |> Join.tableName
    let f = typeof<'a> |> Reflection.getFields
    let fieldNames = f |> List.map (sprintf "%s.%s" table)
    fieldNames, f.Head

let private createSplitOn (xs:string list) = xs |> String.concat ","

type Deconstructor() =
    static member select<'a> (q:SelectQuery) =
        let fields = typeof<'a> |> Reflection.getFields
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let query = Evaluators.evalSelectQuery fields meta q
        let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
        query, pars
        
    static member select<'a,'b> (q:SelectQuery) =
        let joinsArray = q.Joins |> Array.ofList
        let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
        let fieldsTwo, splitOn = extractFieldsAndSplit<'b> joinsArray.[0]
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let query = Evaluators.evalSelectQuery (fieldsOne @ fieldsTwo) meta q
        let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
        query, pars, createSplitOn [splitOn]
        
    static member select<'a,'b,'c> (q:SelectQuery) =
        let joinsArray = q.Joins |> Array.ofList
        let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
        let fieldsTwo, splitOn1 = extractFieldsAndSplit<'b> joinsArray.[0]
        let fieldsThree, splitOn2 = extractFieldsAndSplit<'c> joinsArray.[1]
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let query = Evaluators.evalSelectQuery (fieldsOne @ fieldsTwo @ fieldsThree) meta q
        let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
        query, pars, createSplitOn [splitOn1;splitOn2]
    
    static member internal insert (q:InsertQuery<_>,fields, outputFields) =
        let query = Evaluators.evalInsertQuery fields outputFields q
        let pars =
            q.Values
            |> List.map (Reflection.getValues >> List.zip fields)
            |> List.mapi (fun i values ->
                values |> List.map (fun (key,value) -> sprintf "%s%i" key i, Reflection.boxify value))
            |> List.collect id
            |> Map.ofList
        query, pars
    
    static member insert (q:InsertQuery<'a>) =
        let fields = typeof<'a> |> Reflection.getFields
        Deconstructor.insert(q, fields, []) 
        
    static member insertOutput<'Input, 'Output> (q:InsertQuery<'Input>) =
        let fields = typeof<'Input> |> Reflection.getFields
        let outputFields = typeof<'Output> |> Reflection.getFields
        Deconstructor.insert(q, fields, outputFields) 
       
    static member internal update (q:UpdateQuery<_>, fields, outputFields) =
        let values = Reflection.getValues q.Value |> List.map Reflection.boxify
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let pars = (WhereAnalyzer.extractWhereParams meta) @ (List.zip fields values) |> Map.ofList
        let query = Evaluators.evalUpdateQuery fields outputFields meta q
        query, pars
        
    static member update<'a> (q:UpdateQuery<'a>) =
        let fields = typeof<'a> |> Reflection.getFields
        Deconstructor.update(q, fields, []) 
        
    static member updateOutput<'Input, 'Output> (q:UpdateQuery<'Input>) =
        let fields = typeof<'Input> |> Reflection.getFields
        let outputFields = typeof<'Output> |> Reflection.getFields
        Deconstructor.update(q, fields, outputFields) 

    static member internal delete (q:DeleteQuery, outputFields) =
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let pars = (WhereAnalyzer.extractWhereParams meta) |> Map.ofList
        let query = Evaluators.evalDeleteQuery outputFields meta q
        query, pars
        
    static member delete (q:DeleteQuery) =
        Deconstructor.delete(q, [])

    static member deleteOutput<'Output> (q:DeleteQuery) =
        let outputFields = typeof<'Output> |> Reflection.getFields
        Deconstructor.delete(q, outputFields)

type System.Data.IDbConnection with

    member this.SelectAsync<'a> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Deconstructor.select<'a>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsync<'a,'b> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Deconstructor.select<'a,'b>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,('a * 'b)>(query, (fun x y -> x, y), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsync<'a,'b,'c> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Deconstructor.select<'a,'b,'c>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,'c,('a * 'b * 'c)>(query, (fun x y z -> x, y, z), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsyncOption<'a,'b> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Deconstructor.select<'a,'b>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,('a * 'b option)>(query, (fun x y -> x, Reflection.makeOption y), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsyncOption<'a,'b,'c> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Deconstructor.select<'a,'b,'c>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,'c,('a * 'b option * 'c option)>(query, (fun x y z -> x, Reflection.makeOption y, Reflection.makeOption z), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.InsertAsync<'a> (q:InsertQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, values = q |> Deconstructor.insert<'a>
        if logFunction.IsSome then (query, values) |> logFunction.Value
        this.ExecuteAsync(query, values, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.InsertOutputAsync<'Input, 'Output> (q:InsertQuery<'Input>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Deconstructor.insertOutput<'Input, 'Output>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'Output>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.UpdateAsync<'a> (q:UpdateQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Deconstructor.update<'a>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.ExecuteAsync(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.UpdateOutputAsync<'Input, 'Output> (q:UpdateQuery<'Input>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Deconstructor.updateOutput<'Input, 'Output>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'Output>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.DeleteAsync (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Deconstructor.delete
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.ExecuteAsync(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.DeleteOutputAsync<'Output> (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Deconstructor.deleteOutput<'Output>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'Output>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)
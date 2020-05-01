module Dapper.FSharp.MSSQL

open System.Text
open Dapper.FSharp

module private WhereAnalyzer =

    type FieldWhereMetadata = {
        Key : string * ColumnComparison
        Name : string
        ParameterName : string
    }

    let extractWhereParams (meta:FieldWhereMetadata list) =
        let fn (m:FieldWhereMetadata) =
            match m.Key |> snd with
            | Eq p | Ne p | Gt p
            | Lt p | Ge p | Le p -> (m.ParameterName, p) |> Some
            | In p | NotIn p -> (m.ParameterName, p :> obj) |> Some
            | Like str -> (m.ParameterName, str :> obj) |> Some
            | IsNull | IsNotNull -> None
        meta
        |> List.choose fn

    let normalizeParamName (s:string) =
        s.Replace(".","_")

    let rec getWhereMetadata (meta:FieldWhereMetadata list) (w:Where)  =
        match w with
        | Empty -> meta
        | Column (field, comp) ->

            let parName =
                meta
                |> List.filter (fun x -> x.Name = field)
                |> List.length
                |> fun l -> sprintf "Where_%s%i" field (l + 1)
                |> normalizeParamName

            meta @ [{ Key = (field, comp); Name = field; ParameterName = parName }]
        | Binary(w1, _, w2) -> [w1;w2] |> List.fold getWhereMetadata meta
        | Unary(_, w) -> w |> getWhereMetadata meta

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
            let withField op = sprintf "%s %s @%s" fieldMeta.Name op fieldMeta.ParameterName
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

    let evalGroupBy (cols:GroupBy list) =
        cols
        |> List.map (fun (GroupByColumn column) -> column)
        |> String.concat ", "

    let evalPagination (pag:Pagination) =
        match pag with
        | Skip x when x <= 0 -> ""
        | Skip o -> sprintf "OFFSET %i ROWS" o
        | SkipTake(o,f) -> sprintf "OFFSET %i ROWS FETCH NEXT %i ROWS ONLY" o f

    let evalJoins (joins:Join list) =
        let sb = StringBuilder()
        let evalJoin = function
            | InnerJoin(table,colName,equalsTo) -> sprintf " INNER JOIN %s ON %s.%s=%s" table table colName equalsTo
            | LeftJoin(table,colName,equalsTo) -> sprintf " LEFT JOIN %s ON %s.%s=%s" table table colName equalsTo
        joins |> List.map evalJoin |> List.iter (sb.Append >> ignore)
        sb.ToString()

    let evalSelectQuery fields meta (q:SelectQuery) =
        let fieldNames = fields |> String.concat ", "
        // basic query
        let distinct = if q.Distinct then "DISTINCT " else ""
        let sb = StringBuilder(sprintf "SELECT %s%s FROM %s" distinct fieldNames q.Table)
        // joins
        let joins = evalJoins q.Joins
        if joins.Length > 0 then sb.Append joins |> ignore
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        // group by
        let groupBy = evalGroupBy q.GroupBy
        if groupBy.Length > 0 then sb.Append (sprintf " GROUP BY %s" groupBy) |> ignore
        // order by
        let orderBy = evalOrderBy q.OrderBy
        if orderBy.Length > 0 then sb.Append (sprintf " ORDER BY %s" orderBy) |> ignore
        // pagination
        let pagination = evalPagination q.Pagination
        if pagination.Length > 0 then sb.Append (sprintf " %s" pagination) |> ignore
        sb.ToString()

    let evalInsertQuery fields (q:InsertQuery<_>) =
        let fieldNames = fields |> String.concat ", " |> sprintf "(%s)"
        let values = fields |> List.map (sprintf "@%s") |> String.concat ", " |> sprintf "(%s)"
        sprintf "INSERT INTO %s %s VALUES %s" q.Table fieldNames values

    // Separate function because dapper QueryAsync does not allow using 'a list type parameters
    // We need to make each parameter name unique
    let evalOutputInsertQuery fields outputFields (q:InsertQuery<_>) =
        let fieldNames = fields |> String.concat ", " |> sprintf "(%s)"
        let values =
            q.Values
            |> List.mapi (fun i _ -> fields |> List.map (fun field -> sprintf "@%s%i" field i ) |> String.concat ", " |> sprintf "(%s)")
            |> String.concat ", "
        match outputFields with
        | [] ->
            sprintf "INSERT INTO %s %s VALUES %s" q.Table fieldNames values
        | outputFields ->
            let outputFieldNames = outputFields |> List.map (sprintf "INSERTED.%s") |> String.concat ", "
            sprintf "INSERT INTO %s %s OUTPUT %s VALUES %s" q.Table fieldNames outputFieldNames values

    let evalUpdateQuery fields outputFields meta (q:UpdateQuery<'a>) =
        // basic query
        let pairs = fields |> List.map (fun x -> sprintf "%s=@%s" x x) |> String.concat ", "
        let baseQuery =
            match outputFields with
            | [] ->
                sprintf "UPDATE %s SET %s" q.Table pairs
            | outputFields ->
                let outputFieldNames = outputFields |> List.map (sprintf "INSERTED.%s") |> String.concat ", "
                sprintf "UPDATE %s SET %s OUTPUT %s" q.Table pairs outputFieldNames
        let sb = StringBuilder(baseQuery)
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        sb.ToString()

    let evalDeleteQuery outputFields meta (q:DeleteQuery) =
        let baseQuery =
            match outputFields with
            | [] ->
                sprintf "DELETE FROM %s" q.Table
            | outputFields ->
                let outputFieldNames = outputFields |> List.map (sprintf "DELETED.%s") |> String.concat ", "
                sprintf "DELETE FROM %s OUTPUT %s" q.Table outputFieldNames
        // basic query
        let sb = StringBuilder(baseQuery)
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        sb.ToString()

module private Reflection =
    open System

    let mkSome (typ:Type) arg =
        let unionType = typedefof<option<_>>.MakeGenericType typ
        let meth = unionType.GetMethod("Some")
        meth.Invoke(null, [|arg|])

    let makeOption<'a> (v:obj) : Option<'a> =
        match box v with
        | null -> None
        | x -> mkSome typeof<'a> x :?> Option<_>

    let getFields (t:Type) =
        FSharp.Reflection.FSharpType.GetRecordFields(t)
        |> Array.map (fun x -> x.Name)
        |> Array.toList

    let getValues r =
        FSharp.Reflection.FSharpValue.GetRecordFields r
        |> Array.toList

    let boxify (x : obj) =
        match x with
        | null -> null
        | _ -> match x.GetType().GetProperty("Value") with
               | null -> x
               | prop -> prop.GetValue(x)

module private Preparators =

    let prepareAggregates aggr =
        let comparableName (column:string) (alias:string) =
            match column.Split '.' with
            | [| _ |] -> alias.ToLowerInvariant()
            | [| table; _ |] -> sprintf "%s.%s" (table.ToLowerInvariant()) (alias.ToLowerInvariant())
            | _ -> failwith "Aggregate column format should be either <table>.<column> or <column>"

        aggr |> Seq.map (function
        | Count (column,alias) -> comparableName column alias, sprintf "COUNT(%s) AS %s" column alias
        | Avg (column,alias) -> comparableName column alias, sprintf "AVG(%s) AS %s" column alias
        | Sum (column,alias) -> comparableName column alias, sprintf "SUM(%s) AS %s" column alias
        | Min (column,alias) -> comparableName column alias, sprintf "MIN(%s) AS %s" column alias
        | Max (column,alias) -> comparableName column alias, sprintf "MAX(%s) AS %s" column alias)
        |> Seq.toList

    let replaceFieldWithAggregate (aggr:(string * string) list) (field:string) =
        let lowerField = field.ToLowerInvariant()
        aggr 
        |> List.tryPick (fun (aggrColumn,func) -> if aggrColumn = lowerField then Some func else None)
        |> Option.defaultValue field

    let prepareSelect<'a> (aggr:Aggregate seq) (q:SelectQuery) =
        let aggregates = prepareAggregates aggr
        let fields =
            typeof<'a> 
            |> Reflection.getFields
            |> Seq.map (replaceFieldWithAggregate aggregates)
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let query = Evaluators.evalSelectQuery fields meta q
        let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
        query, pars

    let private extractFieldsAndSplit<'a> (j:Join) (aggregates:(string * string) list) =
        let table = j |> Join.tableName
        let f = typeof<'a> |> Reflection.getFields
        let fieldNames = f |> List.map (sprintf "%s.%s" table >> replaceFieldWithAggregate aggregates)
        fieldNames, f.Head

    let private createSplitOn (xs:string list) = xs |> String.concat ","

    let prepareSelectTuple2<'a,'b> (aggr:Aggregate seq) (q:SelectQuery) =
        let aggregates = prepareAggregates aggr
        let joinsArray = q.Joins |> Array.ofList
        let fieldsOne = 
            typeof<'a> 
            |> Reflection.getFields 
            |> List.map (sprintf "%s.%s" q.Table >> replaceFieldWithAggregate aggregates)
        let fieldsTwo, splitOn = extractFieldsAndSplit<'b> joinsArray.[0] aggregates
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let query = Evaluators.evalSelectQuery (fieldsOne @ fieldsTwo) meta q
        let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
        query, pars, createSplitOn [splitOn]

    let prepareSelectTuple3<'a,'b,'c> (aggr:Aggregate seq) (q:SelectQuery) =
        let aggregates = prepareAggregates aggr
        let joinsArray = q.Joins |> Array.ofList
        let fieldsOne =
            typeof<'a> 
            |> Reflection.getFields 
            |> List.map (sprintf "%s.%s" q.Table >> replaceFieldWithAggregate aggregates)
        let fieldsTwo, splitOn1 = extractFieldsAndSplit<'b> joinsArray.[0] aggregates
        let fieldsThree, splitOn2 = extractFieldsAndSplit<'c> joinsArray.[1] aggregates
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let query = Evaluators.evalSelectQuery (fieldsOne @ fieldsTwo @ fieldsThree) meta q
        let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
        query, pars, createSplitOn [splitOn1;splitOn2]

    let prepareInsert (q:InsertQuery<'a>) =
        let fields = typeof<'a> |> Reflection.getFields
        let query = Evaluators.evalInsertQuery fields q
        query, q.Values

    let prepareOutputInsert<'Input, 'Output> (q:InsertQuery<'Input>) =
        let fields = typeof<'Input> |> Reflection.getFields
        let outputFields = typeof<'Output> |> Reflection.getFields
        let query = Evaluators.evalOutputInsertQuery fields outputFields q
        let pars =
            q.Values
            |> List.map (Reflection.getValues >> List.zip fields)
            |> List.mapi (fun i values ->
                values |> List.map (fun (key,value) -> sprintf "%s%i" key i, Reflection.boxify value))
            |> List.collect id
            |> Map.ofList
        query, pars

    let prepareUpdate<'a> (q:UpdateQuery<'a>) =
        let fields = typeof<'a> |> Reflection.getFields
        let values = Reflection.getValues q.Value |> List.map Reflection.boxify
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let pars = (WhereAnalyzer.extractWhereParams meta) @ (List.zip fields values) |> Map.ofList
        let query = Evaluators.evalUpdateQuery fields [] meta q
        query, pars

    let prepareOutputUpdate<'Input, 'Output> (q:UpdateQuery<'Input>) =
        let fields = typeof<'Input> |> Reflection.getFields
        let outputFields = typeof<'Output> |> Reflection.getFields
        let values = Reflection.getValues q.Value |> List.map Reflection.boxify
        // extract metadata
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let pars = (WhereAnalyzer.extractWhereParams meta) @ (List.zip fields values) |> Map.ofList
        let query = Evaluators.evalUpdateQuery fields outputFields meta q
        query, pars

    let prepareDelete (q:DeleteQuery) =
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let pars = (WhereAnalyzer.extractWhereParams meta) |> Map.ofList
        let query = Evaluators.evalDeleteQuery [] meta q
        query, pars

    let prepareOutputDelete<'Output> (q:DeleteQuery) =
        let outputFields = typeof<'Output> |> Reflection.getFields
        let meta = WhereAnalyzer.getWhereMetadata [] q.Where
        let pars = (WhereAnalyzer.extractWhereParams meta) |> Map.ofList
        let query = Evaluators.evalDeleteQuery outputFields meta q
        query, pars

open System
open System.Data
open Dapper

type System.Data.IDbConnection with

    member this.SelectAsync<'a> (q:SelectQuery, ?aggr:Aggregate seq, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Preparators.prepareSelect<'a> (aggr |> Option.defaultValue Seq.empty)
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsync<'a,'b> (q:SelectQuery, ?aggr:Aggregate seq, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Preparators.prepareSelectTuple2<'a,'b> (aggr |> Option.defaultValue Seq.empty)
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,('a * 'b)>(query, (fun x y -> x, y), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsync<'a,'b,'c> (q:SelectQuery, ?aggr:Aggregate seq, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Preparators.prepareSelectTuple3<'a,'b,'c> (aggr |> Option.defaultValue Seq.empty)
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,'c,('a * 'b * 'c)>(query, (fun x y z -> x, y, z), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsyncOption<'a,'b> (q:SelectQuery, ?aggr:Aggregate seq, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Preparators.prepareSelectTuple2<'a,'b> (aggr |> Option.defaultValue Seq.empty)
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,('a * 'b option)>(query, (fun x y -> x, Reflection.makeOption y), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.SelectAsyncOption<'a,'b,'c> (q:SelectQuery, ?aggr:Aggregate seq, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars, splitOn = q |> Preparators.prepareSelectTuple3<'a,'b,'c> (aggr |> Option.defaultValue Seq.empty)
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'a,'b,'c,('a * 'b option * 'c option)>(query, (fun x y z -> x, Reflection.makeOption y, Reflection.makeOption z), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.InsertAsync<'a> (q:InsertQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, values = q |> Preparators.prepareInsert
        if logFunction.IsSome then (query, values) |> logFunction.Value
        this.ExecuteAsync(query, values, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.InsertOutputAsync<'Input, 'Output> (q:InsertQuery<'Input>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Preparators.prepareOutputInsert<'Input, 'Output>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'Output>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.UpdateAsync<'a> (q:UpdateQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Preparators.prepareUpdate<'a>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.ExecuteAsync(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.UpdateOutputAsync<'Input, 'Output> (q:UpdateQuery<'Input>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Preparators.prepareOutputUpdate<'Input, 'Output>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'Output>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.DeleteAsync (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Preparators.prepareDelete
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.ExecuteAsync(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

    member this.DeleteOutputAsync<'Output> (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        let query, pars = q |> Preparators.prepareOutputDelete<'Output>
        if logFunction.IsSome then (query, pars) |> logFunction.Value
        this.QueryAsync<'Output>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)
module internal Dapper.FSharp.GenericDeconstructor

let private extractFieldsAndSplit<'a> (j:Join) =
    let table = j |> Join.tableName
    let f = typeof<'a> |> Reflection.getFields
    let fieldNames = f |> List.map (sprintf "%s.%s" table)
    fieldNames, f.Head

let private createSplitOn (xs:string list) = xs |> String.concat ","

let select1<'a> evalSelectQuery (q:SelectQuery) =
    let fields = typeof<'a> |> Reflection.getFields
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let query : string = evalSelectQuery fields meta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
    query, pars

let select2<'a,'b> evalSelectQuery (q:SelectQuery) =
    let joinsArray = q.Joins |> Array.ofList
    let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
    let fieldsTwo, splitOn = extractFieldsAndSplit<'b> joinsArray.[0]
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let query : string = evalSelectQuery (fieldsOne @ fieldsTwo) meta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
    query, pars, createSplitOn [splitOn]

let select3<'a,'b, 'c> evalSelectQuery (q:SelectQuery) =
    let joinsArray = q.Joins |> Array.ofList
    let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
    let fieldsTwo, splitOn1 = extractFieldsAndSplit<'b> joinsArray.[0]
    let fieldsThree, splitOn2 = extractFieldsAndSplit<'c> joinsArray.[1]
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let query : string = evalSelectQuery (fieldsOne @ fieldsTwo @ fieldsThree) meta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList
    query, pars, createSplitOn [splitOn1;splitOn2]

let private _insert evalInsertQuery (q:InsertQuery<_>) fields outputFields =
    let query : string = evalInsertQuery fields outputFields q
    let pars =
        q.Values
        |> List.map (Reflection.getValuesForFields fields >> List.zip fields)
        |> List.mapi (fun i values ->
            values |> List.map (fun (key,value) -> sprintf "%s%i" key i, Reflection.boxify value))
        |> List.collect id
        |> Map.ofList
    query, pars

let insert evalInsertQuery (q:InsertQuery<'a>) =
    let fields = 
        match q.Fields with
        | [] -> typeof<'a> |> Reflection.getFields
        | fields -> fields
    _insert evalInsertQuery q fields []

let insertOutput<'Input, 'Output> evalInsertQuery (q:InsertQuery<'Input>) =
    let fields = 
        match q.Fields with
        | [] -> typeof<'Input> |> Reflection.getFields
        | fields -> fields
    let outputFields = typeof<'Output> |> Reflection.getFields
    _insert evalInsertQuery q fields outputFields

let private _update evalUpdateQuery (q:UpdateQuery<_>) fields (outputFields:string list) =
    let values = 
        match q.Value with
        | Some value -> Reflection.getValuesForFields fields q.Value |> List.map Reflection.boxify
        | None -> q.SetColumns |> List.map snd
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let pars = (WhereAnalyzer.extractWhereParams meta) @ (List.zip fields values) |> Map.ofList
    let query : string = evalUpdateQuery fields outputFields meta q
    query, pars
    
let update<'a> evalUpdateQuery (q:UpdateQuery<'a>) =
    let fields =
        match q.Value, q.Fields, q.SetColumns with
        | Some _, [], _ -> typeof<'a> |> Reflection.getFields
        | Some _, fields, _ -> fields
        | None, _, setCols -> setCols |> List.map fst
    _update evalUpdateQuery q fields [] 
    
let updateOutput<'Input, 'Output> evalUpdateQuery (q:UpdateQuery<'Input>) =
    let fields = 
        match q.Value, q.Fields, q.SetColumns with
        | Some _, [], _ -> typeof<'Input> |> Reflection.getFields
        | Some _, fields, _ -> fields
        | None, _, setCols -> setCols |> List.map fst
    let outputFields = typeof<'Output> |> Reflection.getFields
    _update evalUpdateQuery q fields outputFields 

let private _delete evalDeleteQuery (q:DeleteQuery) outputFields =
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let pars = (WhereAnalyzer.extractWhereParams meta) |> Map.ofList
    let query : string = evalDeleteQuery outputFields meta q
    query, pars
    
let delete evalDeleteQuery (q:DeleteQuery) = _delete evalDeleteQuery q []

let deleteOutput<'Output> evalDeleteQuery (q:DeleteQuery) =
    let outputFields = typeof<'Output> |> Reflection.getFields
    _delete evalDeleteQuery q outputFields
module Dapper.FSharp.MySQL

open System.Text
open System.Linq
open Dapper.FSharp

let private specialStrings = [ "*" ]

let private inQuotes (s:string) =
    s.Split('.')
    |> Array.map (fun x -> if specialStrings.Contains(x) then x else sprintf "`%s`" x)
    |> String.concat "."

let private safeTableName schema table =
    match schema, table with
    | None, table -> table |> inQuotes
    | Some schema, table -> (schema |> inQuotes) + "." + (table |> inQuotes)

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
        | Expr expr -> expr
        | Column (field, comp) ->
            let fieldMeta = meta |> List.find (fun x -> x.Key = (field,comp))
            let withField op = sprintf "%s %s @%s" (inQuotes fieldMeta.Name) op fieldMeta.ParameterName
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
            | NotLike _ -> withField "NOT LIKE"
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
        | { Take = None; Skip = o } -> sprintf "LIMIT %i, %i" o (System.UInt64.MaxValue)
        | { Take = Some f; Skip = o } -> sprintf "LIMIT %i, %i" o f
    
    let evalJoins (joins:Join list) =
        let sb = StringBuilder()
        let evalJoin = function
            | InnerJoin(table,colName,equalsTo) -> sprintf " INNER JOIN %s ON %s.%s=%s" (inQuotes table) (inQuotes table) (inQuotes colName) (inQuotes equalsTo)
            | LeftJoin(table,colName,equalsTo) -> sprintf " LEFT JOIN %s ON %s.%s=%s" (inQuotes table) (inQuotes table) (inQuotes colName) (inQuotes equalsTo)
        joins |> List.map evalJoin |> List.iter (sb.Append >> ignore)
        sb.ToString()
    
    let evalAggregates (ags:Aggregate list) =
        let comparableName (column:string) (alias:string) =
            match column.Split '.' with
            | [| _ |] -> alias
            | [| table; _ |] -> sprintf "%s.%s" table alias
            | _ -> failwith "Aggregate column format should be either <table>.<column> or <column>"

        ags |> List.map (function
        | Count (column,alias) -> comparableName column alias, sprintf "COUNT(%s) AS %s" (inQuotes column) (inQuotes alias)
        | Avg (column,alias) -> comparableName column alias, sprintf "AVG(%s) AS %s" (inQuotes column) (inQuotes alias)
        | Sum (column,alias) -> comparableName column alias, sprintf "SUM(%s) AS %s" (inQuotes column) (inQuotes alias)
        | Min (column,alias) -> comparableName column alias, sprintf "MIN(%s) AS %s" (inQuotes column) (inQuotes alias)
        | Max (column,alias) -> comparableName column alias, sprintf "MAX(%s) AS %s" (inQuotes column) (inQuotes alias)
        )

    let replaceFieldWithAggregate (aggr:(string * string) list) (field:string) =
        aggr 
        |> List.tryPick (fun (aggrColumn, replace) -> if aggrColumn = field then Some replace else None)
        |> Option.defaultValue (inQuotes field)
    
    let evalGroupBy (cols:string list) =
        cols
        |> List.map inQuotes
        |> String.concat ", "
        
    let evalSelectQuery fields meta (q:SelectQuery) =
        let aggregates = q.Aggregates |> evalAggregates
        let fieldNames =
            fields
            |> List.map (replaceFieldWithAggregate aggregates)
            |> String.concat ", "
        // distinct
        let distinct = if q.Distinct then "DISTINCT " else ""
        // basic query
        let sb = StringBuilder(sprintf "SELECT %s%s FROM %s" distinct fieldNames (safeTableName q.Schema q.Table))
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
    
    let evalInsertQuery fields _ (q:InsertQuery<_>) =
        let fieldNames = fields |> List.map inQuotes |> String.concat ", " |> sprintf "(%s)"
        let values =
            q.Values
            |> List.mapi (fun i _ -> fields |> List.map (fun field -> sprintf "@%s%i" field i ) |> String.concat ", " |> sprintf "(%s)")
            |> String.concat ", "
        sprintf "INSERT INTO %s %s VALUES %s" (safeTableName q.Schema q.Table) fieldNames values
        
    let evalUpdateQuery fields _ meta (q:UpdateQuery<'a>) =
        // basic query
        let pairs = fields |> List.map (fun x -> sprintf "%s=@%s" (inQuotes x) x) |> String.concat ", "
        let baseQuery = sprintf "UPDATE %s SET %s" (safeTableName q.Schema q.Table) pairs
        let sb = StringBuilder(baseQuery)
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        sb.ToString()

    let evalDeleteQuery _ meta (q:DeleteQuery) =
        let baseQuery = sprintf "DELETE FROM %s" (safeTableName q.Schema q.Table)
        // basic query
        let sb = StringBuilder(baseQuery)
        // where
        let where = evalWhere meta q.Where
        if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
        sb.ToString()

[<AbstractClass;Sealed>]
type Deconstructor =
    static member select<'a> (q:SelectQuery) = q |> GenericDeconstructor.select1<'a> Evaluators.evalSelectQuery
    static member select<'a,'b> (q:SelectQuery) = q |> GenericDeconstructor.select2<'a,'b> Evaluators.evalSelectQuery
    static member select<'a,'b,'c> (q:SelectQuery) = q |> GenericDeconstructor.select3<'a,'b,'c> Evaluators.evalSelectQuery
    static member insert (q:InsertQuery<'a>) = q |> GenericDeconstructor.insert Evaluators.evalInsertQuery
    static member update<'a> (q:UpdateQuery<'a>) = q |> GenericDeconstructor.update<'a> Evaluators.evalUpdateQuery
    static member delete (q:DeleteQuery) = q |> GenericDeconstructor.delete Evaluators.evalDeleteQuery

open System.Data

type IDbConnection with

    member this.SelectAsync<'a> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.select<'a> |> IDbConnection.query1<'a> this trans timeout logFunction
      
    member this.SelectAsync<'a,'b> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.select<'a,'b> |> IDbConnection.query2<'a,'b> this trans timeout logFunction

    member this.SelectAsync<'a,'b,'c> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.select<'a,'b,'c> |> IDbConnection.query3<'a,'b,'c> this trans timeout logFunction

    member this.SelectAsyncOption<'a,'b> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.select<'a,'b>|> IDbConnection.query2Option<'a,'b> this trans timeout logFunction

    member this.SelectAsyncOption<'a,'b,'c> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.select<'a,'b,'c> |> IDbConnection.query3Option<'a,'b,'c> this trans timeout logFunction

    member this.InsertAsync<'a> (q:InsertQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.insert<'a> |> IDbConnection.execute this trans timeout logFunction
        
    member this.UpdateAsync<'a> (q:UpdateQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.update<'a> |> IDbConnection.execute this trans timeout logFunction

    member this.DeleteAsync (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction) =
        q |> Deconstructor.delete |> IDbConnection.execute this trans timeout logFunction
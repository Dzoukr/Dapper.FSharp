module internal Dapper.FSharp.PostgreSQL.Evaluator

open Dapper.FSharp.PostgreSQL
open Dapper.FSharp.PostgreSQL.WhereAnalyzer
open Dapper.FSharp.PostgreSQL.JoinAnalyzer
open System.Linq
open System.Text

let private specialStrings = [ "*" ]

let private inQuotes (s:string) =
    s.Split('.')
    |> Array.map (fun x -> if specialStrings.Contains(x) then x else sprintf "\"%s\"" x)
    |> String.concat "."

let private safeTableName schema table =
    match schema, table with
    | None, table -> table |> inQuotes
    | Some schema, table -> (schema |> inQuotes) + "." + (table |> inQuotes)


let evalBinary = function
    | And -> "AND"
    | Or -> "OR"

let evalOrderDirection = function
    | Asc -> "ASC"
    | Desc -> "DESC"

let rec evalWhere (meta:FieldWhereMetadata list) (w:Where) =
    match w with
    | Empty -> ""
    | Expr expr -> expr
    | Column (field, comp) ->
        let fieldMeta = meta |> List.find (fun x -> x.Key = (field,comp))
        let fieldName = fieldMeta.Name |> inQuotes
        let withField op = sprintf "%s %s (@%s)" fieldName op fieldMeta.ParameterName
        match comp with
        | Eq _ -> withField "="
        | Ne _ -> withField "<>"
        | Gt _ -> withField ">"
        | Lt _ -> withField "<"
        | Ge _ -> withField ">="
        | Le _ -> withField "<="
        | In _ -> withField "= ANY"
        | NotIn _ -> withField "<> ALL"
        | Like _ -> withField "LIKE"
        | NotLike _ -> withField "NOT LIKE"
        | ILike _ -> withField "ILIKE"
        | NotILike _ -> withField "NOT ILIKE"
        | IsNull -> sprintf "%s IS NULL" fieldName
        | IsNotNull -> sprintf "%s IS NOT NULL" fieldName
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
    |> List.map (fun (n,s) -> sprintf "%s %s" (inQuotes n) (evalOrderDirection s))
    |> String.concat ", "

let evalPagination (pag:Pagination) =
    match pag with
    | { Take = None; Skip = x } when x <= 0 -> ""
    | { Take = None; Skip = o } -> sprintf "LIMIT ALL OFFSET %i" o
    | { Take = Some f; Skip = o } -> sprintf "LIMIT %i OFFSET %i" f o

let buildJoinType (meta:JoinMetadata list) = function
    | EqualsToColumn eqToCol -> (inQuotes eqToCol)
    | EqualsToConstant con -> meta |> List.find (fun x -> x.Key = con) |> (fun x -> "@" + x.ParameterName)

let buildJoinOnMany meta joinType tableName (joinList: List<string * JoinType>) =
    joinList
    |> List.map (fun (colName, jt) -> sprintf "%s.%s=%s" (inQuotes tableName) (inQuotes colName) (buildJoinType meta jt))
    |> List.reduce (fun s1 s2 -> s1 + " AND " + s2 )
    |> sprintf " %s JOIN %s ON %s" joinType (inQuotes tableName)

let evalJoins (meta:JoinMetadata list) (joins:Join list) =
    let sb = StringBuilder()
    let evalJoin = function
        | InnerJoin(table, list) -> buildJoinOnMany meta "INNER" table list
        | LeftJoin(table, list) -> buildJoinOnMany meta "LEFT" table list
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
    | CountDistinct (column,alias) -> comparableName column alias, sprintf "COUNT(DISTINCT %s) AS %s" (inQuotes column) (inQuotes alias)
    | Avg (column,alias) -> comparableName column alias, sprintf "AVG(%s) AS %s" (inQuotes column) (inQuotes alias)
    | Sum (column,alias) -> comparableName column alias, sprintf "SUM(%s) AS %s" (inQuotes column) (inQuotes alias)
    | Min (column,alias) -> comparableName column alias, sprintf "MIN(%s) AS %s" (inQuotes column) (inQuotes alias)
    | Max (column,alias) -> comparableName column alias, sprintf "MAX(%s) AS %s" (inQuotes column) (inQuotes alias)
    )

let replaceFieldWithAggregate (aggr:(string * string) list) (field:string) =
    aggr
    |> List.tryPick (fun (aggrColumn, replace) ->
        match aggrColumn.Split '.', field.Split '.' with
        | [| _; c |], [| _ |] when c = field -> Some replace // aggrColumn is <table>.<column> but field is <column>
        | [| _ |], [| _; c |] when aggrColumn = c -> Some replace // aggrColumn is <column> but field is <table>.<column>
        | _ when aggrColumn = field -> Some replace
        | _ -> None)
    |> Option.defaultValue (inQuotes field)

let evalGroupBy (cols:string list) =
    cols
    |> List.map inQuotes
    |> String.concat ", "

let evalSelectQuery fields meta joinMeta (q:SelectQuery) =
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
    let joins = evalJoins joinMeta q.Joins
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

let evalInsertQuery fields outputFields (q:InsertQuery<_>) =
    let fieldNames = fields |> List.map inQuotes |> String.concat ", " |> sprintf "(%s)"
    let values =
        q.Values
        |> List.mapi (fun i _ -> fields |> List.map (fun field -> sprintf "@%s%i" field i ) |> String.concat ", " |> sprintf "(%s)")
        |> String.concat ", "
    match outputFields with
    | [] ->
        sprintf "INSERT INTO %s %s VALUES %s" (safeTableName q.Schema q.Table) fieldNames values
    | outputFields ->
        let outputFieldNames = outputFields |> List.map inQuotes |> String.concat ", "
        sprintf "INSERT INTO %s %s VALUES %s RETURNING %s" (safeTableName q.Schema q.Table) fieldNames values outputFieldNames

let evalUpdateQuery fields outputFields meta (q:UpdateQuery<'a>) =
    // basic query
    let pairs = fields |> List.map (fun x -> sprintf "%s=@%s" (inQuotes x) x) |> String.concat ", "
    let baseQuery = sprintf "UPDATE %s SET %s" (safeTableName q.Schema q.Table) pairs
    let sb = StringBuilder(baseQuery)
    // where
    let where = evalWhere meta q.Where
    if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
    match outputFields with
    | [] -> sb.ToString()
    | outputFields ->
        let outputFieldNames = outputFields |> List.map inQuotes |> String.concat ", "
        sprintf " RETURNING %s" outputFieldNames
        |> sb.Append
        |> string

let evalDeleteQuery outputFields meta (q:DeleteQuery) =
    let baseQuery = sprintf "DELETE FROM %s" (safeTableName q.Schema q.Table)
    // basic query
    let sb = StringBuilder(baseQuery)
    // where
    let where = evalWhere meta q.Where
    if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
    match outputFields with
    | [] -> sb.ToString()
    | outputFields ->
        let outputFieldNames = outputFields |> List.map inQuotes |> String.concat ", "
        sprintf " RETURNING %s" outputFieldNames
        |> sb.Append
        |> string
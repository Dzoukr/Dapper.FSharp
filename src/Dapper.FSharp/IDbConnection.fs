module internal Dapper.FSharp.IDbConnection

open System.Data
open Dapper

type LogFn = string * Map<string, obj> -> unit

let query1<'a> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    |> this.QueryAsync<'a>

let query2<'a,'b> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,('a * 'b)>(query, (fun a b -> a,b), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout = timeout)

let query3<'a,'b,'c> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,('a * 'b * 'c)>(query, (fun a b c -> a,b,c), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout =timeout)
    
let query4<'a,'b,'c,'d> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,'d,('a * 'b * 'c * 'd)>(query, (fun a b c d -> a,b,c,d), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout =timeout)
    
let query5<'a,'b,'c,'d,'e> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,'d,'e,('a * 'b * 'c * 'd * 'e)>(query, (fun a b c d e -> a,b,c,d,e), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout =timeout)

let query2Option<'a,'b> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,('a * 'b option)>(query, (fun x y -> x, Reflection.makeOption y), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout = timeout)

let query3Option<'a,'b,'c> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,('a * 'b option * 'c option)>(query, (fun x y z -> x, Reflection.makeOption y, Reflection.makeOption z), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout = timeout)

let query4Option<'a,'b,'c,'d> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a, 'b, 'c, 'd, ('a * 'b option * 'c option * 'd option)>(
    query,
    (fun a b c d -> a, Reflection.makeOption b, Reflection.makeOption c, Reflection.makeOption d),
    pars,
    splitOn = splitOn,
    ?transaction = trans,
    ?commandTimeout = timeout
)
    
let query5Option<'a,'b,'c,'d,'e> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a, 'b, 'c, 'd, 'e, ('a * 'b option * 'c option * 'd option * 'e option)>(
        query,
        (fun a b c d e ->
            a, Reflection.makeOption b, Reflection.makeOption c, Reflection.makeOption d, Reflection.makeOption e),
        pars,
        splitOn = splitOn,
        ?transaction = trans,
        ?commandTimeout = timeout
    )
let execute (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, values) =
    if logFunction.IsSome then (query, values) |> logFunction.Value
    CommandDefinition(query, values, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    |> this.ExecuteAsync
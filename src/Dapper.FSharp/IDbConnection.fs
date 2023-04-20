module internal Dapper.FSharp.IDbConnection

open System.Data
open Dapper

type LogFn = string * Map<string, obj> -> unit

let query1<'a> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    |> this.QueryAsync<'a>

let query2<'a,'b> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a,'b,('a * 'b)>(cmd, (fun a b -> a,b), splitOn)

let query3<'a,'b,'c> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a,'b,'c,('a * 'b * 'c)>(cmd, (fun a b c -> a,b,c), splitOn)
    
let query4<'a,'b,'c,'d> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a,'b,'c,'d,('a * 'b * 'c * 'd)>(cmd, (fun a b c d -> a,b,c,d), splitOn)
    
let query5<'a,'b,'c,'d,'e> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a,'b,'c,'d,'e,('a * 'b * 'c * 'd * 'e)>(cmd, (fun a b c d e -> a,b,c,d,e), splitOn)

let query2Option<'a,'b> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a,'b,('a * 'b option)>(cmd, (fun x y -> x, Reflection.makeOption y), splitOn)

let query3Option<'a,'b,'c> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a,'b,'c,('a * 'b option * 'c option)>(cmd, (fun x y z -> x, Reflection.makeOption y, Reflection.makeOption z), splitOn)

let query4Option<'a,'b,'c,'d> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a, 'b, 'c, 'd, ('a * 'b option * 'c option * 'd option)>(
    cmd,
    (fun a b c d -> a, Reflection.makeOption b, Reflection.makeOption c, Reflection.makeOption d),
    splitOn)
    
let query5Option<'a,'b,'c,'d,'e> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    let cmd = CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    this.QueryAsync<'a, 'b, 'c, 'd, 'e, ('a * 'b option * 'c option * 'd option * 'e option)>(
        cmd,
        (fun a b c d e ->
            a, Reflection.makeOption b, Reflection.makeOption c, Reflection.makeOption d, Reflection.makeOption e),
        splitOn)
let execute (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, values) =
    if logFunction.IsSome then (query, values) |> logFunction.Value
    CommandDefinition(query, values, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    |> this.ExecuteAsync
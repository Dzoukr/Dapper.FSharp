﻿module internal Dapper.FSharp.IDbConnection

open System.Data
open Dapper

type LogFn = string * Map<string, obj> -> unit

let query1<'a> (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, pars) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    CommandDefinition(query, pars, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    |> this.QueryAsync<'a>

let query2<'a,'b> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,('a * 'b)>(query, (fun x y -> x, y), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout = timeout)

let query3<'a,'b, 'c> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,('a * 'b * 'c)>(query, (fun x y z -> x, y, z), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout =timeout)

let query2Option<'a,'b> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,('a * 'b option)>(query, (fun x y -> x, Reflection.makeOption y), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout = timeout)

let query3Option<'a,'b,'c> (this:IDbConnection) trans timeout (logFunction:LogFn option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,('a * 'b option * 'c option)>(query, (fun x y z -> x, Reflection.makeOption y, Reflection.makeOption z), pars, splitOn = splitOn, ?transaction = trans, ?commandTimeout = timeout)

let execute (this:IDbConnection) trans timeout cancellationToken (logFunction:LogFn option) (query, values) =
    if logFunction.IsSome then (query, values) |> logFunction.Value
    CommandDefinition(query, values, ?transaction = trans, ?commandTimeout = timeout, ?cancellationToken = cancellationToken)
    |> this.ExecuteAsync
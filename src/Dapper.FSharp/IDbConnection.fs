module internal Dapper.FSharp.IDbConnection

open System.Data
open Dapper

let query1<'a> (this:IDbConnection) trans timeout (logFunction:(_ -> unit) option) (query, pars) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a>(query, pars, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

let query2<'a,'b> (this:IDbConnection) trans timeout (logFunction:(_ -> unit) option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,('a * 'b)>(query, (fun x y -> x, y), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

let query3<'a,'b, 'c> (this:IDbConnection) trans timeout (logFunction:(_ -> unit) option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,('a * 'b * 'c)>(query, (fun x y z -> x, y, z), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

let query2Option<'a,'b> (this:IDbConnection) trans timeout (logFunction:(_ -> unit) option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,('a * 'b option)>(query, (fun x y -> x, Reflection.makeOption y), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

let query3Option<'a,'b,'c> (this:IDbConnection) trans timeout (logFunction:(_ -> unit) option) (query, pars, splitOn) =
    if logFunction.IsSome then (query, pars) |> logFunction.Value
    this.QueryAsync<'a,'b,'c,('a * 'b option * 'c option)>(query, (fun x y z -> x, Reflection.makeOption y, Reflection.makeOption z), pars, splitOn = splitOn, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

let execute (this:IDbConnection) trans timeout (logFunction:(_ -> unit) option) (query, values) =
    if logFunction.IsSome then (query, values) |> logFunction.Value
    this.ExecuteAsync(query, values, transaction = Option.toObj trans, commandTimeout = Option.toNullable timeout)

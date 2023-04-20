[<AutoOpen>]
module Dapper.FSharp.SQLite.IDbConnection

open Dapper.FSharp
open Dapper.FSharp.SQLite
open System.Data
open System.Threading

type IDbConnection with

    member this.SelectAsync<'a> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a> |> IDbConnection.query1<'a> this trans timeout cancellationToken logFunction 

    member this.SelectAsync<'a,'b> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b> |> IDbConnection.query2<'a,'b> this trans timeout cancellationToken logFunction

    member this.SelectAsync<'a,'b,'c> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b,'c> |> IDbConnection.query3<'a,'b,'c> this trans timeout cancellationToken logFunction

    member this.SelectAsync<'a,'b,'c,'d> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b,'c,'d> |> IDbConnection.query4<'a,'b,'c,'d> this trans timeout cancellationToken logFunction

    member this.SelectAsync<'a,'b,'c,'d,'e> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b,'c,'d,'e> |> IDbConnection.query5<'a,'b,'c,'d,'e> this trans timeout cancellationToken logFunction

    member this.SelectAsyncOption<'a,'b> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b>|> IDbConnection.query2Option<'a,'b> this trans timeout cancellationToken logFunction

    member this.SelectAsyncOption<'a,'b,'c> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b,'c> |> IDbConnection.query3Option<'a,'b,'c> this trans timeout cancellationToken logFunction

    member this.SelectAsyncOption<'a,'b,'c,'d> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b,'c,'d> |> IDbConnection.query4Option<'a,'b,'c,'d> this trans timeout cancellationToken logFunction

    member this.SelectAsyncOption<'a,'b,'c,'d,'e> (q:SelectQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.select<'a,'b,'c,'d,'e> |> IDbConnection.query5Option<'a,'b,'c,'d,'e> this trans timeout cancellationToken logFunction
        
    member this.InsertAsync<'a> (q:InsertQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken ) =
        q |> Deconstructor.insert<'a> |> IDbConnection.execute this trans timeout cancellationToken logFunction

    member this.InsertOrReplaceAsync<'a> (q:InsertQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken ) =
        q |> Deconstructor.insertOrReplace<'a> |> IDbConnection.execute this trans timeout cancellationToken logFunction

    member this.UpdateAsync<'a> (q:UpdateQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.update<'a> |> IDbConnection.execute this trans timeout cancellationToken logFunction

    member this.DeleteAsync (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.delete |> IDbConnection.execute this trans timeout cancellationToken logFunction

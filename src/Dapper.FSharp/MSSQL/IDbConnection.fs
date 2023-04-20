[<AutoOpen>]
module Dapper.FSharp.MSSQL.IDbConnection

open Dapper.FSharp
open Dapper.FSharp.MSSQL
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

    member this.InsertOutputAsync<'Input, 'Output> (q:InsertQuery<'Input>, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.insertOutput<'Input, 'Output> |> IDbConnection.query1<'Output> this trans timeout cancellationToken logFunction

    member this.UpdateAsync<'a> (q:UpdateQuery<'a>, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.update<'a> |> IDbConnection.execute this trans timeout cancellationToken logFunction

    member this.UpdateOutputAsync<'Input, 'Output> (q:UpdateQuery<'Input>, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.updateOutput<'Input, 'Output> |> IDbConnection.query1<'Output> this trans timeout cancellationToken logFunction

    member this.DeleteAsync (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.delete |> IDbConnection.execute this trans timeout cancellationToken logFunction

    member this.DeleteOutputAsync<'Output> (q:DeleteQuery, ?trans:IDbTransaction, ?timeout:int, ?logFunction, ?cancellationToken : CancellationToken) =
        q |> Deconstructor.deleteOutput<'Output> |> IDbConnection.query1<'Output> this trans timeout cancellationToken logFunction


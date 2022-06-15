module Dapper.FSharp.Tests.Extensions

open FSharp.Control.Tasks
open Dapper

type System.Data.IDbConnection with
    member this.ExecuteIgnore (cmd:string) =
        task {
            let! _ = this.ExecuteAsync(cmd)
            return ()
        }
    member this.ExecuteCatchIgnore (cmd:string) =
        task {
            try
                do! this.ExecuteIgnore(cmd)
            with _ -> return ()
        }

module Expect =
    open System.Threading.Tasks

    [<RequiresExplicitTypeArguments>]
    /// Expects the passed function to throw `'texn`.
    let throwsT2Task<'texn, 'texn2 when 'texn :> exn and 'texn2 :> exn> (f : unit -> Task) (message : string) = task {
        let thrown = task {
            try
                do! f ()
                return None
            with e ->
                return Some e
        }
        match! thrown with
        | Some e when e.GetType() <> typeof<'texn> && e.GetType() <> typeof<'texn2> ->
            Expecto.Tests.failtestf "%s. Expected f to throw an exn of type %s, but one of type %s was thrown."
                message
                (typeof<'texn>.FullName)
                (e.GetType().FullName)
        | Some _ -> ()
        | _ -> Expecto.Tests.failtestf "%s. Expected f to throw." message
    }
    /// Expects OperationCanceledException or TaskCanceledException for a cancelled task
    let throwsTaskCanceledException (f : unit -> Task) (message : string) = task {
        return! throwsT2Task<System.OperationCanceledException, TaskCanceledException> f message
    }
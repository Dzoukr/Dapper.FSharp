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
open System.IO
open Fake.Core
open Fake.IO
open Fake.IO.FileSystemOperators
open Fake.Core.TargetOperators

open BuildHelpers
open BuildTools

initializeContext()

let clean proj = [ proj </> "bin"; proj </> "obj" ] |> Shell.cleanDirs

let createNuget proj =
    clean proj
    Tools.dotnet "restore --no-cache" proj
    Tools.dotnet "pack -c Release" proj

let publishNuget proj =
    createNuget proj
    let nugetKey =
        match Environment.environVarOrNone "NUGET_KEY" with
        | Some nugetKey -> nugetKey
        | None -> failwith "The Nuget API key must be set in a NUGET_KEY environmental variable"
    let nupkg =
        Directory.GetFiles(proj </> "bin" </> "Release")
        |> Seq.head
        |> Path.GetFullPath
    Tools.dotnet (sprintf "nuget push %s -s nuget.org -k %s" nupkg nugetKey) proj


Target.create "Pack" (fun _ -> "src" </> "Dapper.FSharp" |> createNuget)
Target.create "Publish" (fun _ -> "src" </> "Dapper.FSharp" |> publishNuget)
Target.create "Test" (fun _ -> Tools.dotnet "run" ("tests" </> "Dapper.FSharp.Tests"))

let dependencies = [
    "Test" ==> "Pack"
    "Test" ==> "Publish"
]

[<EntryPoint>]
let main args = runOrDefault "Test" args
module BuildHelpers

open Fake.Core
open Fake.DotNet

let initializeContext () =
    let execContext = Context.FakeExecutionContext.Create false "build.fsx" [ ]
    Context.setExecutionContext (Context.RuntimeContext.Fake execContext)

module Proc =
    module Parallel =
        open System

        let locker = obj()

        let colors =
            [| ConsoleColor.Blue
               ConsoleColor.Yellow
               ConsoleColor.Magenta
               ConsoleColor.Cyan
               ConsoleColor.DarkBlue
               ConsoleColor.DarkYellow
               ConsoleColor.DarkMagenta
               ConsoleColor.DarkCyan |]

        let print color (colored: string) (line: string) =
            lock locker
                (fun () ->
                    let currentColor = Console.ForegroundColor
                    Console.ForegroundColor <- color
                    Console.Write colored
                    Console.ForegroundColor <- currentColor
                    Console.WriteLine line)

        let onStdout index name (line: string) =
            let color = colors.[index % colors.Length]
            if isNull line then
                print color $"{name}: --- END ---" ""
            else if String.isNotNullOrEmpty line then
                print color $"{name}: " line

        let onStderr name (line: string) =
            let color = ConsoleColor.Red
            if isNull line |> not then
                print color $"{name}: " line

        let redirect (index, (name, createProcess)) =
            createProcess
            |> CreateProcess.redirectOutputIfNotRedirected
            |> CreateProcess.withOutputEvents (onStdout index name) (onStderr name)

        let printStarting indexed =
            for (index, (name, c: CreateProcess<_>)) in indexed do
                let color = colors.[index % colors.Length]
                let wd =
                    c.WorkingDirectory
                    |> Option.defaultValue ""
                let exe = c.Command.Executable
                let args = c.Command.Arguments.ToStartInfo
                print color $"{name}: {wd}> {exe} {args}" ""

        let run cs =
            cs
            |> Seq.toArray
            |> Array.indexed
            |> fun x -> printStarting x; x
            |> Array.map redirect
            |> Array.Parallel.map Proc.run

let createProcess exe arg dir =
    CreateProcess.fromRawCommandLine exe arg
    |> CreateProcess.withWorkingDirectory dir
    |> CreateProcess.ensureExitCode

let run proc arg dir =
    proc arg dir
    |> Proc.run
    |> ignore

let runParallel processes =
    processes
    |> Proc.Parallel.run
    |> ignore

let runOrDefault defTarget args =
    try
        match args with
        | [| target |] -> Target.runOrDefault target
        | _ -> Target.runOrDefault defTarget
        0
    with e ->
        printfn "%A" e
        1

module Dapper.FSharp.Reflection

open System
open System.Reflection

let mkSome (typ:Type) arg =
    let unionType = typedefof<option<_>>.MakeGenericType typ
    let meth = unionType.GetMethod("Some")
    meth.Invoke(null, [|arg|])

let makeOption<'a> (v:obj) : Option<'a> =
    match box v with
    | null -> None
    | x -> mkSome typeof<'a> x :?> Option<_>

let getFields (t:Type) =
    FSharp.Reflection.FSharpType.GetRecordFields(t, BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.NonPublic)
    |> Array.map (fun x -> x.Name)
    |> Array.toList

let getValues r =
    FSharp.Reflection.FSharpValue.GetRecordFields r
    |> Array.toList

let getValuesForFields fields r = 
    FSharp.Reflection.FSharpType.GetRecordFields(r.GetType(), BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.NonPublic)
    |> Array.filter (fun p -> fields |> List.contains(p.Name))
    |> Array.map (fun p -> p.GetValue r)
    |> Array.toList

let boxify (x : obj) =
    match x with
    | null -> null
    | _ -> match x.GetType().GetProperty("Value") with
           | null -> x
           | prop -> prop.GetValue(x)

type ReflectiveListBuilder = 
        static member BuildList<'a> (args: obj list) = 
            [ for a in args do yield a :?> 'a ]
        static member BuildResizeArray<'a> args = args |> ReflectiveListBuilder.BuildList<'a> |> ResizeArray            
        static member BuildTypedList lType (args: obj list) = 
            typeof<ReflectiveListBuilder>
                .GetMethod("BuildList")
                .MakeGenericMethod([|lType|])
                .Invoke(null, [|args|])
        static member BuildTypedResizeArray lType (args: obj list) = 
            typeof<ReflectiveListBuilder>
                .GetMethod("BuildResizeArray")
                .MakeGenericMethod([|lType|])
                .Invoke(null, [|args|])
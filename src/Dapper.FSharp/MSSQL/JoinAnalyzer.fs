module internal Dapper.FSharp.MSSQL.JoinAnalyzer


open System.Linq
open Dapper.FSharp

type JoinMetadata = {
    Key : obj
    ParameterName : string
}

let private extractJoins (joins:Join list) =
    joins |> List.collect (fun j ->
        match j with
        | InnerJoin (_, j) -> j
        | LeftJoin (_, j) -> j
    )

let private extractConstants (jx:(string * JoinType) list) =
    jx |> List.collect (fun (t,j) ->
        match j with
        | EqualsToConstant c -> [ c ]
        | EqualsToColumn _ -> [  ]
    )
    |> List.distinct

let getJoinMetadata (joins:Join list)  =
    joins
    |> extractJoins
    |> extractConstants
    |> List.mapi (fun i c -> { Key = c; ParameterName = sprintf "JoinConst_%i" i })

let addToMap (meta:JoinMetadata list) (m:Map<string, obj>)  =
    meta
    |> List.map (fun x -> x.ParameterName, x.Key)
    |> List.fold (fun (acc:Map<_,_>) item -> acc.Add item) m
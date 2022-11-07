module internal Dapper.FSharp.PostgreSQL.WhereAnalyzer

open Dapper.FSharp.Reflection
open Dapper.FSharp.PostgreSQL
open System.Linq

type FieldWhereMetadata = {
    Key : string * ColumnComparison
    Name : string
    ParameterName : string
}

let extractWhereParams (meta:FieldWhereMetadata list) =
    let fn (m:FieldWhereMetadata) =
        match m.Key |> snd with
        | Eq p | Ne p | Gt p
        | Lt p | Ge p | Le p -> (m.ParameterName, p) |> Some
        | In p | NotIn p ->
            match p |> Seq.tryHead with
            | Some h ->
                let x = ReflectiveListBuilder.BuildTypedResizeArray (h.GetType()) p
                (m.ParameterName, x) |> Some
            | None -> (m.ParameterName, p.ToArray() :> obj) |> Some
        | Like str
        | ILike str
        | NotILike str
        | NotLike str -> (m.ParameterName, str :> obj) |> Some
        | IsNull | IsNotNull -> None
    meta
    |> List.choose fn

let normalizeParamName (s:string) = s.Replace(".","_")

let rec getWhereMetadata (meta:FieldWhereMetadata list) (w:Where)  =
    match w with
    | Empty -> meta
    | Expr _ -> meta
    | Column (field, comp) ->
        let parName =
            meta
            |> List.filter (fun x -> System.String.Equals(x.Name, field, System.StringComparison.OrdinalIgnoreCase))
            |> List.length
            |> fun l -> sprintf "Where_%s%i" field (l + 1)
            |> normalizeParamName

        { Key = (field, comp); Name = field; ParameterName = parName } :: meta
        |> List.rev
    | Binary(w1, _, w2) -> [w1;w2] |> List.fold getWhereMetadata meta
    | Unary(_, w) -> w |> getWhereMetadata meta
[<AutoOpen>]
module Dapper.FSharp.MySQL.Deconstructor

open Dapper.FSharp.MySQL.Evaluator

[<AbstractClass;Sealed>]
type Deconstructor =
    static member select<'a> (q:SelectQuery) = q |> GenericDeconstructor.select1<'a> evalSelectQuery
    static member select<'a,'b> (q:SelectQuery) = q |> GenericDeconstructor.select2<'a,'b> evalSelectQuery
    static member select<'a,'b,'c> (q:SelectQuery) = q |> GenericDeconstructor.select3<'a,'b,'c> evalSelectQuery
    static member insert (q:InsertQuery<'a>) = q |> GenericDeconstructor.insert evalInsertQuery
    static member update<'a> (q:UpdateQuery<'a>) = q |> GenericDeconstructor.update<'a> evalUpdateQuery
    static member delete (q:DeleteQuery) = q |> GenericDeconstructor.delete evalDeleteQuery
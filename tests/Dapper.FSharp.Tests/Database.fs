module Dapper.FSharp.Tests.Database

open System
open System.Threading.Tasks

let taskToList (t:Task<seq<'a>>) = t |> Async.AwaitTask |> Async.RunSynchronously |> Seq.toList

module Persons =

    type View = {
        Id : Guid
        FirstName : string
        LastName : string
        Position : int
        DateOfBirth : DateTime option
    }

    type ViewRequired = {
        Id : Guid
        FirstName : string
        LastName : string
        Position : int
    }

    module View =
        let generate x =
            [1..x]
            |> List.map (fun x ->
                {
                    Id = Guid.NewGuid()
                    FirstName = sprintf "First_%i" x
                    LastName = sprintf "Last_%i" x
                    DateOfBirth = if x%2=0 then None else Some (x |> float |> DateTime.Today.AddDays)
                    Position = x
                }
            )
    
module Dogs =

    type View = {
        OwnerId : Guid
        Nickname : string
    }

    module View =
        let generate1to1 (owners:Persons.View list) =
            owners
            |> List.mapi (fun i x ->
                {
                    OwnerId = x.Id
                    Nickname = sprintf "Dog_%i" i
                }
            )

        let generate1toN count (owner:Persons.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    OwnerId = owner.Id
                    Nickname = sprintf "Dog_%i" i
                }
            )

module DogsWeights =

    type View = {
        DogNickname : string
        Year : int16
        Weight : int16
    }

    module View =
        let generate1to1 (dogs:Dogs.View list) =
            dogs
            |> List.mapi (fun i x ->
                {
                    DogNickname = x.Nickname
                    Year = 2010s + (int16 i)
                    Weight = 10s + (int16 i)
                }
            )

        let generate1toN count (dog:Dogs.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    DogNickname = dog.Nickname
                    Year = 2010s + (int16 i)
                    Weight = 10s + (int16 i)
                }

            )
            
module Issues =
    
    module PersonsSimple =
    
        type View = {
            Id : int
            Name : string
            Desc : string
        }
        
        module View =
            let generate x =
                [1..x]
                |> List.map (fun x ->
                    {
                        Id = x
                        Name = sprintf "Name_%i" x
                        Desc = sprintf "Desc_%i" x
                    }
                )
                
    module PersonsSimpleDescs =
    
        type View = {
            Id : int
            Desc : string
        }
        
        module View =
            let generate x =
                [1..x]
                |> List.map (fun x ->
                    {
                        Id = x
                        Desc = sprintf "Desc_%i" x
                    }
                )

module Articles =
    
    type View = {
        Id : int option
        Title : string
    }
    
module Group =
    
    type View = {
        Id : int
        Name : string
    }
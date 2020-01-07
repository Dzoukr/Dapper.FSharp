module Dapper.FSharp.OptionTypes

open Dapper
open System
    
type OptionHandler<'T>() =
    inherit SqlMapper.TypeHandler<option<'T>>()

    override __.SetValue(param, value) = 
        let valueOrNull = 
            match value with
            | Some x -> box x
            | None -> null

        param.Value <- valueOrNull    

    override __.Parse value =
        if isNull value || value = box DBNull.Value 
        then None
        else Some (value :?> 'T)
        
let register() =
    SqlMapper.AddTypeHandler (OptionHandler<Guid>())
    SqlMapper.AddTypeHandler (OptionHandler<int64>())
    SqlMapper.AddTypeHandler (OptionHandler<int>())
    SqlMapper.AddTypeHandler (OptionHandler<int16>())
    SqlMapper.AddTypeHandler (OptionHandler<float>())
    SqlMapper.AddTypeHandler (OptionHandler<decimal>())
    SqlMapper.AddTypeHandler (OptionHandler<string>())
    SqlMapper.AddTypeHandler (OptionHandler<char>())
    SqlMapper.AddTypeHandler (OptionHandler<DateTime>())
    SqlMapper.AddTypeHandler (OptionHandler<DateTimeOffset>())
    SqlMapper.AddTypeHandler (OptionHandler<bool>())
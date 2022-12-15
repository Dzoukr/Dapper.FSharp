module Dapper.FSharp.OptionTypes

open Dapper
open System

type OptionHandler<'T>() =
    inherit SqlMapper.TypeHandler<option<'T>>()

    override _.SetValue(param, value) =
        let valueOrNull =
            match value with
            | Some x -> box x
            | None -> null

        param.Value <- valueOrNull

    override _.Parse value =
        if isNull value || value = box DBNull.Value
        then None
        else Some (value :?> 'T)

let register() =
    SqlMapper.AddTypeHandler (OptionHandler<Guid>())
    SqlMapper.AddTypeHandler (OptionHandler<byte>())
    SqlMapper.AddTypeHandler (OptionHandler<int16>())
    SqlMapper.AddTypeHandler (OptionHandler<int>())
    SqlMapper.AddTypeHandler (OptionHandler<int64>())
    SqlMapper.AddTypeHandler (OptionHandler<uint16>())
    SqlMapper.AddTypeHandler (OptionHandler<uint>())
    SqlMapper.AddTypeHandler (OptionHandler<uint64>())    
    SqlMapper.AddTypeHandler (OptionHandler<float>())
    SqlMapper.AddTypeHandler (OptionHandler<decimal>())
    SqlMapper.AddTypeHandler (OptionHandler<float32>())
    SqlMapper.AddTypeHandler (OptionHandler<string>())
    SqlMapper.AddTypeHandler (OptionHandler<char>())
    SqlMapper.AddTypeHandler (OptionHandler<DateTime>())
    SqlMapper.AddTypeHandler (OptionHandler<DateTimeOffset>())
    SqlMapper.AddTypeHandler (OptionHandler<bool>())
    SqlMapper.AddTypeHandler (OptionHandler<TimeSpan>())
    SqlMapper.AddTypeHandler (OptionHandler<byte[]>())

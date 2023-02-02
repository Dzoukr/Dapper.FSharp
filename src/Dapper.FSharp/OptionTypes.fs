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

/// A TypeHandler that will use a parser (string -> 'T)
type TypeHandlerWithParser<'T>(parser) =
    inherit SqlMapper.TypeHandler<'T>()
        override _.SetValue(parameter, value) = parameter.Value <- value
        override _.Parse(value: obj) = 
            match value with
            | :? string as str -> parser str
            | :? 'T as theType -> theType
            | _ -> failwith $"Expected string but got type '{value.GetType().Name}'"

/// A TypeHandler that will use a converter (obj -> 'T)
type TypeHandlerWithConverter<'T>(converter) =
    inherit SqlMapper.TypeHandler<'T>()
        override _.SetValue(parameter, value) = parameter.Value <- value
        override _.Parse(value: obj) = converter value

/// A TypeHandler that will use a parser (string -> 'T) for option types
type OptionTypeHandlerWithParser<'T>(parser) =
    inherit SqlMapper.TypeHandler<Option<'T>>()
        override _.SetValue(parameter, value) =
            match value with
            | Some x -> parameter.Value <- x
            | None -> parameter.Value <- DBNull.Value

        override _.Parse(value) = 
            match value with
            | null -> None
            | _ -> Some(parser(value :?> string))

/// A TypeHandler that will use a converter (obj -> 'T) for option types
type OptionTypeHandlerWithConverter<'T>(converter) =
    inherit SqlMapper.TypeHandler<Option<'T>>()
        override _.SetValue(parameter, value) =
            match value with
            | Some x -> parameter.Value <- x
            | None -> parameter.Value <- DBNull.Value

        override _.Parse(value) = 
            match value with
            | null -> None
            | _ -> Some(converter(value))

[<Obsolete("""Registering shared Option type handlers is deprecated and will be removed in next version.
Please use function from a database vendor-scoped namespace.
Example: Dapper.FSharp.MSSQL.OptionTypes.register()""")>]
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

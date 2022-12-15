/// https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/types
module Dapper.FSharp.SQLite.TypeHandlers

open Dapper
open System

/// A TypeHandler that will use a parser (string -> 'T)
type SqliteTypeHandlerWithParser<'T>(parser) =
    inherit SqlMapper.TypeHandler<'T>()
        override __.SetValue(parameter, value) = parameter.Value <- value
        override __.Parse(value: obj) = 
            match value with
            | :? string as str -> parser str
            | :? 'T as theType -> theType
            | _ -> failwith $"Expected string but got type '{value.GetType().Name}'"

/// A TypeHandler that will use a converter (obj -> 'T)
type SqliteTypeHandlerWithConverter<'T>(converter) =
    inherit SqlMapper.TypeHandler<'T>()
        override __.SetValue(parameter, value) = parameter.Value <- value
        override __.Parse(value: obj) = converter value

/// A TypeHandler that will use a parser (string -> 'T) for option types
type SqliteOptionTypeHandlerWithParser<'T>(parser) =
    inherit SqlMapper.TypeHandler<Option<'T>>()
        override __.SetValue(parameter, value) =
            match value with
            | Some x -> parameter.Value <- x
            | None -> parameter.Value <- DBNull.Value

        override __.Parse(value) = 
            match value with
            | null -> None
            | _ -> Some(parser(value :?> string))

/// A TypeHandler that will use a converter (obj -> 'T) for option types
type SqliteOptionTypeHandlerWithConverter<'T>(converter) =
    inherit SqlMapper.TypeHandler<Option<'T>>()
        override __.SetValue(parameter, value) =
            match value with
            | Some x -> parameter.Value <- x
            | None -> parameter.Value <- DBNull.Value

        override __.Parse(value) = 
            match value with
            | null -> None
            | _ -> Some(converter(value))

type SqliteBooleanHandler()              = inherit SqliteTypeHandlerWithConverter<Boolean>(Convert.ToBoolean)
type SqliteByteHandler()                 = inherit SqliteTypeHandlerWithConverter<Byte>(Convert.ToByte)
type SqliteDateOnlyHandler()             = inherit SqliteTypeHandlerWithParser<DateOnly>(DateOnly.Parse)
type SqliteDateTimeHandler()             = inherit SqliteTypeHandlerWithParser<DateTime>(DateTime.Parse)
type SqliteDateTimeOffsetHandler()       = inherit SqliteTypeHandlerWithParser<DateTimeOffset>(DateTimeOffset.Parse)
type SqliteGuidHandler()                 = inherit SqliteTypeHandlerWithParser<Guid>(Guid.Parse)
type SqliteInt16Handler()                = inherit SqliteTypeHandlerWithConverter<Int16>(Convert.ToInt16)
type SqliteInt32Handler()                = inherit SqliteTypeHandlerWithConverter<Int32>(Convert.ToInt32)
type SqliteSByteHandler()                = inherit SqliteTypeHandlerWithConverter<SByte>(Convert.ToSByte)
type SqliteTimeSpanHandler()             = inherit SqliteTypeHandlerWithParser<TimeSpan>(TimeSpan.Parse)
type SqliteUInt16Handler()               = inherit SqliteTypeHandlerWithConverter<UInt16>(Convert.ToUInt16)
type SqliteUInt32Handler()               = inherit SqliteTypeHandlerWithConverter<UInt32>(Convert.ToUInt32)

type SqliteOptionBooleanHandler()        = inherit SqliteOptionTypeHandlerWithConverter<Boolean>(Convert.ToBoolean)
type SqliteOptionByteHandler()           = inherit SqliteOptionTypeHandlerWithConverter<Byte>(Convert.ToByte)
type SqliteOptionDateOnlyHandler()       = inherit SqliteOptionTypeHandlerWithParser<DateOnly>(DateOnly.Parse)
type SqliteOptionDateTimeHandler()       = inherit SqliteOptionTypeHandlerWithParser<DateTime>(DateTime.Parse)
type SqliteOptionDateTimeOffsetHandler() = inherit SqliteOptionTypeHandlerWithParser<DateTimeOffset>(DateTimeOffset.Parse)
type SqliteOptionGuidHandler()           = inherit SqliteOptionTypeHandlerWithParser<Guid>(Guid.Parse)
type SqliteOptionInt16Handler()          = inherit SqliteOptionTypeHandlerWithConverter<Int16>(Convert.ToInt16)
type SqliteOptionInt32Handler()          = inherit SqliteOptionTypeHandlerWithConverter<Int32>(Convert.ToInt32)
type SqliteOptionSByteHandler()          = inherit SqliteOptionTypeHandlerWithConverter<SByte>(Convert.ToSByte)
type SqliteOptionTimeSpanHandler()       = inherit SqliteOptionTypeHandlerWithParser<TimeSpan>(TimeSpan.Parse)
type SqliteOptionUInt16Handler()         = inherit SqliteOptionTypeHandlerWithConverter<UInt16>(Convert.ToUInt16)
type SqliteOptionUInt32Handler()         = inherit SqliteOptionTypeHandlerWithConverter<UInt32>(Convert.ToUInt32)

let addSQLiteTypeHandlers() =
    SqlMapper.AddTypeHandler(new SqliteBooleanHandler())
    SqlMapper.AddTypeHandler(new SqliteByteHandler())
    SqlMapper.AddTypeHandler(new SqliteDateTimeHandler())
    SqlMapper.AddTypeHandler(new SqliteDateOnlyHandler())
    SqlMapper.AddTypeHandler(new SqliteDateTimeOffsetHandler())
    SqlMapper.AddTypeHandler(new SqliteGuidHandler())
    SqlMapper.AddTypeHandler(new SqliteInt16Handler())
    SqlMapper.AddTypeHandler(new SqliteInt32Handler())
    SqlMapper.AddTypeHandler(new SqliteSByteHandler())
    SqlMapper.AddTypeHandler(new SqliteTimeSpanHandler())
    SqlMapper.AddTypeHandler(new SqliteUInt16Handler())
    SqlMapper.AddTypeHandler(new SqliteUInt32Handler())

    SqlMapper.AddTypeHandler(new SqliteOptionBooleanHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionByteHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionDateTimeHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionDateOnlyHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionDateTimeOffsetHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionGuidHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionInt16Handler())
    SqlMapper.AddTypeHandler(new SqliteOptionInt32Handler())
    SqlMapper.AddTypeHandler(new SqliteOptionSByteHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionTimeSpanHandler())
    SqlMapper.AddTypeHandler(new SqliteOptionUInt16Handler())
    SqlMapper.AddTypeHandler(new SqliteOptionUInt32Handler())

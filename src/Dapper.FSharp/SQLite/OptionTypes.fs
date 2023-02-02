[<RequireQualifiedAccess>]
module Dapper.FSharp.SQLite.OptionTypes

open Dapper
open System
open Dapper.FSharp.OptionTypes

type SqliteBooleanHandler()              = inherit TypeHandlerWithConverter<Boolean>(Convert.ToBoolean)
type SqliteByteHandler()                 = inherit TypeHandlerWithConverter<Byte>(Convert.ToByte)
type SqliteDateOnlyHandler()             = inherit TypeHandlerWithParser<DateOnly>(DateOnly.Parse)
type SqliteDateTimeHandler()             = inherit TypeHandlerWithParser<DateTime>(DateTime.Parse)
type SqliteDateTimeOffsetHandler()       = inherit TypeHandlerWithParser<DateTimeOffset>(DateTimeOffset.Parse)
type SqliteGuidHandler()                 = inherit TypeHandlerWithParser<Guid>(Guid.Parse)
type SqliteInt16Handler()                = inherit TypeHandlerWithConverter<Int16>(Convert.ToInt16)
type SqliteInt32Handler()                = inherit TypeHandlerWithConverter<Int32>(Convert.ToInt32)
type SqliteSByteHandler()                = inherit TypeHandlerWithConverter<SByte>(Convert.ToSByte)
type SqliteTimeSpanHandler()             = inherit TypeHandlerWithParser<TimeSpan>(TimeSpan.Parse)
type SqliteUInt16Handler()               = inherit TypeHandlerWithConverter<UInt16>(Convert.ToUInt16)
type SqliteUInt32Handler()               = inherit TypeHandlerWithConverter<UInt32>(Convert.ToUInt32)

type SqliteOptionBooleanHandler()        = inherit OptionTypeHandlerWithConverter<Boolean>(Convert.ToBoolean)
type SqliteOptionByteHandler()           = inherit OptionTypeHandlerWithConverter<Byte>(Convert.ToByte)
type SqliteOptionDateOnlyHandler()       = inherit OptionTypeHandlerWithParser<DateOnly>(DateOnly.Parse)
type SqliteOptionDateTimeHandler()       = inherit OptionTypeHandlerWithParser<DateTime>(DateTime.Parse)
type SqliteOptionDateTimeOffsetHandler() = inherit OptionTypeHandlerWithParser<DateTimeOffset>(DateTimeOffset.Parse)
type SqliteOptionGuidHandler()           = inherit OptionTypeHandlerWithParser<Guid>(Guid.Parse)
type SqliteOptionInt16Handler()          = inherit OptionTypeHandlerWithConverter<Int16>(Convert.ToInt16)
type SqliteOptionInt32Handler()          = inherit OptionTypeHandlerWithConverter<Int32>(Convert.ToInt32)
type SqliteOptionSByteHandler()          = inherit OptionTypeHandlerWithConverter<SByte>(Convert.ToSByte)
type SqliteOptionTimeSpanHandler()       = inherit OptionTypeHandlerWithParser<TimeSpan>(TimeSpan.Parse)
type SqliteOptionUInt16Handler()         = inherit OptionTypeHandlerWithConverter<UInt16>(Convert.ToUInt16)
type SqliteOptionUInt32Handler()         = inherit OptionTypeHandlerWithConverter<UInt32>(Convert.ToUInt32)

let register() =
    SqlMapper.AddTypeHandler(SqliteBooleanHandler())
    SqlMapper.AddTypeHandler(SqliteByteHandler())
    SqlMapper.AddTypeHandler(SqliteDateTimeHandler())
    SqlMapper.AddTypeHandler(SqliteDateOnlyHandler())
    SqlMapper.AddTypeHandler(SqliteDateTimeOffsetHandler())
    SqlMapper.AddTypeHandler(SqliteGuidHandler())
    SqlMapper.AddTypeHandler(SqliteInt16Handler())
    SqlMapper.AddTypeHandler(SqliteInt32Handler())
    SqlMapper.AddTypeHandler(SqliteSByteHandler())
    SqlMapper.AddTypeHandler(SqliteTimeSpanHandler())
    SqlMapper.AddTypeHandler(SqliteUInt16Handler())
    SqlMapper.AddTypeHandler(SqliteUInt32Handler())

    SqlMapper.AddTypeHandler(SqliteOptionBooleanHandler())
    SqlMapper.AddTypeHandler(SqliteOptionByteHandler())
    SqlMapper.AddTypeHandler(SqliteOptionDateTimeHandler())
    SqlMapper.AddTypeHandler(SqliteOptionDateOnlyHandler())
    SqlMapper.AddTypeHandler(SqliteOptionDateTimeOffsetHandler())
    SqlMapper.AddTypeHandler(SqliteOptionGuidHandler())
    SqlMapper.AddTypeHandler(SqliteOptionInt16Handler())
    SqlMapper.AddTypeHandler(SqliteOptionInt32Handler())
    SqlMapper.AddTypeHandler(SqliteOptionSByteHandler())
    SqlMapper.AddTypeHandler(SqliteOptionTimeSpanHandler())
    SqlMapper.AddTypeHandler(SqliteOptionUInt16Handler())
    SqlMapper.AddTypeHandler(SqliteOptionUInt32Handler())
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL.CreateVerify.Model
{
    /// <summary>
    /// Running list of the different types of column datatypes we use, based on .NET datatypes to store.
    /// This list is for creating database tables and for verifying them.
    /// Basically, you pick one of these for each table column, and the creation/verify logic transposes that to the Postgres-specific type.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum eColDataTypes
    {
        /// <summary>
        /// Default not set state.
        /// </summary>
        notset = 0,

        /// <summary>
        /// Covers .NET Boolean type
        /// </summary>
        boolean = 101,

        /// <summary>
        /// Covers .NET Int32 type
        /// </summary>
        integer = 102,
        /// <summary>
        /// Covers .NET Int64 type
        /// </summary>
        bigint = 103,
        /// <summary>
        /// Covers .NET float type
        /// </summary>
        real = 104,
        /// <summary>
        /// Covers .NET double type
        /// </summary>
        double_precision = 105,
        /// <summary>
        /// Covers .NET decimal type
        /// </summary>
        numeric = 106,

        /// <summary>
        /// Covers a string type, with a max-length.
        /// </summary>
        varchar = 201,
        /// <summary>
        /// Covers the unlimited string type.
        /// No max length is required for this type.
        /// </summary>
        text = 202,

        /// <summary>
        /// Covers the Guid/UUID.
        /// </summary>
        uuid = 301,

        /// <summary>
        /// Timestamp without time zone.
        /// </summary>
        timestamp = 401,
        /// <summary>
        /// Timestamp with time zone.
        /// </summary>
        timestampUTC = 402,

        /// <summary>
        /// UUID primary key
        /// </summary>
        pk_uuid = 501,
        /// <summary>
        /// 32-bit integer primary key
        /// </summary>
        pk_integer = 502,
        /// <summary>
        /// 64-bit integer primary key
        /// </summary>
        pk_bigint = 503,
        /// <summary>
        /// Varchar primary key
        /// Added to support EF Migration Databases, which use varchar pk's.
        /// </summary>
        pk_varchar = 504,
    }

    [JsonConverter(typeof(StringEnumConverter))]
    public enum eErrorType
    {
        /// <summary>
        /// Failed to query/update database server.
        /// </summary>
        DatabaseAccessError,
        /// <summary>
        /// Something failed during layout validation.
        /// </summary>
        ValidationError,
        /// <summary>
        /// Object Not Found in Live Database object.
        /// </summary>
        NotFound,
        /// <summary>
        /// Object different in Live Database.
        /// </summary>
        Different,
        /// <summary>
        /// Object is extra in Live Database.
        /// </summary>
        Extra
    }

    [JsonConverter(typeof(StringEnumConverter))]
    public enum eObjType
    {
        Layout = 1,
        Server = 2,
        Database = 3,
        Table = 4,
        Column = 5,
    }

    /// <summary>
    /// Defines the generation behavior an identity column uses.
    /// NOTE: This is only valid for bigint and integer datatypes.
    /// ByDefault will accept the user-specified value if included in an INSERT.
    /// Always will be created at insert, and error if the value is included in the INSERT statement.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum eIdentityBehavior
    {
        /// <summary>
        /// Undefined behavior state.
        /// </summary>
        UNSET = 0,
        /// <summary>
        /// The column will be auto-generated if no user value was specified on INSERT.
        /// </summary>
        GenerateByDefault = 1,
        /// <summary>
        /// The column will be auto-generated on INSERT.
        /// If the user's insert specifies a value, an error will be generated.
        /// </summary>
        GenerateAlways = 2,
    }
}

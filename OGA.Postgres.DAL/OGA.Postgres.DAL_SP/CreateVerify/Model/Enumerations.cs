using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL_SP.CreateVerify.Model
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
        /// Covers .NET Int32 type
        /// </summary>
        integer = 1,
        /// <summary>
        /// Covers .NET Int64 type
        /// </summary>
        bigint = 2,
        /// <summary>
        /// Covers .NET float type
        /// </summary>
        real = 3,
        /// <summary>
        /// Covers .NET double type
        /// </summary>
        double_precision = 4,
        /// <summary>
        /// Covers .NET decimal type
        /// </summary>
        numeric = 5,

        /// <summary>
        /// Covers a string type, with a max-length.
        /// </summary>
        varchar = 6,
        /// <summary>
        /// Covers the unlimited string type.
        /// No max length is required for this type.
        /// </summary>
        text = 7,

        /// <summary>
        /// Covers the Guid/UUID.
        /// </summary>
        uuid = 8,

        /// <summary>
        /// Timestamp without time zone.
        /// </summary>
        timestamp = 9,
        /// <summary>
        /// Timestamp with time zone.
        /// </summary>
        timestampUTC = 10,

        /// <summary>
        /// UUID primary key
        /// </summary>
        pk_uuid = 11,
        /// <summary>
        /// 32-bit integer primary key
        /// </summary>
        pk_integer = 12,
        /// <summary>
        /// 64-bit integer primary key
        /// </summary>
        pk_bigint = 13,
    }

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

    public enum eObjType
    {
        Layout = 1,
        Server = 2,
        Database = 3,
        Table = 4,
        Column = 5,
    }
}

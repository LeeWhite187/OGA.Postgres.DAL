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
    public enum eColDataTypes
    {
        /// <summary>
        /// Default not set state.
        /// </summary>
        notset,

        /// <summary>
        /// UUID primary key
        /// </summary>
        pk_uuid,
        /// <summary>
        /// 32-bit integer primary key
        /// </summary>
        pk_integer,
        /// <summary>
        /// 64-bit integer primary key
        /// </summary>
        pk_bigint,

        /// <summary>
        /// Timestamp with time zone.
        /// </summary>
        timestampUTC,
        /// <summary>
        /// Timestamp without time zone.
        /// </summary>
        timestamp,

        /// <summary>
        /// Covers .NET Int32 type
        /// </summary>
        integer,
        /// <summary>
        /// Covers .NET Int64 type
        /// </summary>
        bigint,
        /// <summary>
        /// Covers .NET float type
        /// </summary>
        real,
        /// <summary>
        /// Covers .NET double type
        /// </summary>
        double_precision,
        /// <summary>
        /// Covers .NET decimal type
        /// </summary>
        numeric,

        /// <summary>
        /// Covers a string type, with a max-length.
        /// </summary>
        varchar,
        /// <summary>
        /// Covers the unlimited string type.
        /// No max length is required for this type.
        /// </summary>
        text,

        /// <summary>
        /// Covers the Guid/UUID.
        /// </summary>
        uuid,
    }
}

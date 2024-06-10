using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OGA.Postgres
{
    public enum eDBBuiltinRoleAttributes
    {
        /// <summary>
        /// The user/role is a superuser.
        /// This role attribute allows any functionality.
        /// </summary>
        superuser = 1,
        /// <summary>
        /// Role attribute that allows a role/user to create roles.
        /// </summary>
        createrole = 2,
        /// <summary>
        /// Role attribute that allows a role/user to create databases.
        /// </summary>
        createdb = 3,
        /// <summary>
        /// Role attribute that allows a role/user to login to the postgres server.
        /// </summary>
        login = 4,
    }

    /// <summary>
    /// List of privileges for a PostGreSQL data table.
    /// </summary>
    [Flags]
    public enum eTablePrivileges
    {
        /// <summary>
        /// Included in this list as the no privilege entry.
        /// </summary>
        NONE = 0,
        /// <summary>
        /// Allows SELECT from any column, or specific column(s), of a table, view, materialized view, or other table-like object.
        /// Also allows use of COPY TO.
        /// This privilege is also needed to reference existing column values in UPDATE, DELETE, or MERGE.
        /// For sequences, this privilege also allows use of the currval function.
        /// For large objects, this privilege allows the object to be read.
        /// </summary>
        SELECT = 1,
        /// <summary>
        /// Allows INSERT of a new row into a table, view, etc.
        /// Can be granted on specific column(s), in which case only those columns may be assigned
        ///     to in the INSERT command (other columns will therefore receive default values).
        /// Also allows use of COPY FROM.
        /// </summary>
        INSERT = 2,
        /// <summary>
        /// Allows UPDATE of any column, or specific column(s), of a table, view, etc.
        /// SELECT ... FOR UPDATE and SELECT ... FOR SHARE also require this privilege on at least one column, in addition to the SELECT privilege.
        /// For sequences, this privilege allows use of the nextval and setval functions.
        /// For large objects, this privilege allows writing or truncating the object.
        /// </summary>
        UPDATE = 4,
        /// <summary>
        /// Allows DELETE of a row from a table, view, etc.
        /// </summary>
        DELETE = 8,
        /// <summary>
        /// Allows TRUNCATE on a table.
        /// </summary>
        TRUNCATE = 16,
        /// <summary>
        /// Allows creation of a foreign key constraint referencing a table, or specific column(s) of a table.
        /// </summary>
        REFERENCES = 32,
        /// <summary>
        /// Allows creation of a trigger on a table, view, etc.
        /// </summary>
        TRIGGER = 64
    }

    public enum eDatabasePrivileges
    {
        /// <summary>
        /// Allows new schemas and publications to be created within the database.
        /// And, allows trusted extensions to be isntalled within the database.
        /// </summary>
        CREATE = 1,
        /// <summary>
        /// Allows the grantee to connect to the database.
        /// </summary>
        CONNECT = 2,
        /// <summary>
        /// Allows the grantee to create temp tables while using the database.
        /// </summary>
        TEMPORARY = 3,
    }
}

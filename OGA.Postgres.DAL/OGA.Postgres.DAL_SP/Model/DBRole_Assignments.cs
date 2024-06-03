using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.MSSQL.Model
{
    public class DBRole_Assignments
    {
        /// <summary>
        /// Server instance that database resides on
        /// </summary>
        public string ServerName;
        /// <summary>
        /// Database of role assignments
        /// </summary>
        public string DBName;
        /// <summary>
        /// Database User Name
        /// </summary>
        public string UserName;
        /// <summary>
        /// Role assigned to database user
        /// </summary>
        public string GroupName;
        /// <summary>
        /// Login name associated with database user
        /// </summary>
        public string LoginName;
        /// <summary>
        /// Default Database assigned to the user
        /// </summary>
        public string Default_Database_Name;
        /// <summary>
        /// Default Schema assigned to the user
        /// </summary>
        public string Default_Schema_Name;
        /// <summary>
        /// Principal ID of the user
        /// </summary>
        public string Principal_ID;
        /// <summary>
        /// User SID
        /// </summary>
        public string SID;
    }
}

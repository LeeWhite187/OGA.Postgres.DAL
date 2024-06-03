﻿using Microsoft.Extensions.Options;
using OGA.Postgres;
using OGA.Postgres.DAL_SP;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace OGA.Postgres
{
    /// <summary>
    /// Provides methods for managing databases, backup, restore, permissions, and users.
    /// </summary>
    public class Postgres_Tools : IDisposable
    {
        #region Private Fields

        static private string _classname = nameof(Postgres_Tools);

        static private volatile int _instancecounter;

        private OGA.Postgres.Postgres_DAL _dal;

        private bool disposedValue;

        #endregion


        #region Public Properties

        public int InstanceId { get; set; }

        public string Hostname { get; set; }
        public string Database { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        #endregion


        #region ctor / dtor

        public Postgres_Tools()
        {
            _instancecounter++;
            InstanceId = _instancecounter;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                try
                {
                    _dal?.Dispose();
                }
                catch (Exception) { }
                _dal = null;

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~cSQL_Tools()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion


        #region Public Methods

        /// <summary>
        /// Provides a quick ability to test credentials to a PostgreSQL database, without creating a persistent connection.
        /// </summary>
        /// <returns></returns>
        public int TestConnection()
        {
            var dal = new Postgres_DAL();
            dal.Hostname = Hostname;
            dal.Database = Database;
            dal.Username = Username;
            dal.Password = Password;

            return dal.Test_Connection();
        }

        /// <summary>
        /// Public call to create a database user.
        /// The method used will automatically add the 'login' role attribute to the user.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public int CreateUser(string username, string password = "")
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(CreateUser)} - " +
                    $"Attempting to create user...");

                if(!UserNameIsValid(username))
                {
                    // Invalid Username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(CreateUser)} - " +
                        $"Invalid Username.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(CreateUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Add the user...
                string sql = "";
                if(string.IsNullOrEmpty(password))
                    sql = $"CREATE USER {username};";
                else
                    sql = $"CREATE USER {username} WITH PASSWORD '{password}';";
                if (this._dal.Execute_NonQuery(sql) != -1)
                {
                    // Create user command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(CreateUser)} - " +
                        "Create user command failed.");

                    return -2;
                }

                // Check if the user was added...
                var resq = this.Does_Login_Exist(username);
                if(resq != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(CreateUser)} - " +
                        "User not found.");

                    return -3;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(CreateUser)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="userlist">Account login string</param>
        /// <returns></returns>
        public int GetUserList(out List<string> userlist)
        {
            userlist = new List<string>();

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GetUserList)} - " +
                    $"Attempting to check for login to database: {(Database ?? "")}...");

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetUserList)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to get users from the postgres instance...
                string sql = $"SELECT usename " +
                             $"FROM pg_catalog.pg_user " +
                             $"ORDER BY usename ASC;";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetUserList)} - " +
                        "Failed to get users.");

                    return -2;
                }
                // We have a datatable of users.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                    return 1;

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the current user...
                    string sss = r["usename"] + "";
                    userlist.Add(sss);
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GetUserList)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }


        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int Does_Login_Exist(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    $"Attempting to check for login to database: {(Database ?? "")}...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to get users from the postgres instance...
                string sql = $"SELECT usename AS role_name " +
                             $"FROM pg_catalog.pg_user " +
                             $"WHERE usename = '{login}'" +
                             $"ORDER BY role_name desc;";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                        "Failed to get users.");

                    return -2;
                }
                // We have a datatable of users.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No matching user records.
                    return 0;
                }
                // If here, we have user records to check.

                // See if we have a match to the given user.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the current user...
                    string sss = r["role_name"] + "";

                    // See if we have a match...
                    if (sss == login)
                    {
                        // Got a match.

                        return 1;
                    }
                }
                // If here, we didn't find a match.

                return 0;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Public call to delete a database user.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <returns></returns>
        public int DeleteUser(string username)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(DeleteUser)} - " +
                    $"Attempting to delete user...");

                if(string.IsNullOrEmpty(username))
                {
                    // Empty username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DeleteUser)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DeleteUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Delete the user...
                string sql = $"DROP USER IF EXISTS {username};";
                if (this._dal.Execute_NonQuery(sql) != -1)
                {
                    // Delete user command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DeleteUser)} - " +
                        "Delete user command failed.");

                    return -2;
                }

                // Check if the user was deleted...
                var resq = this.Does_Login_Exist(username);
                if(resq != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DeleteUser)} - " +
                        "User was not confirmed as dropped.");

                    return -3;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(DeleteUser)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Public call to change a user password.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public int ChangeUserPassword(string username, string password = "")
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(ChangeUserPassword)} - " +
                    $"Attempting to create user...");

                if(string.IsNullOrEmpty(username))
                {
                    // Empty Username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(ChangeUserPassword)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(ChangeUserPassword)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Change the user password...
                string sql = $"ALTER USER {username} WITH PASSWORD '{password}';";
                if (this._dal.Execute_NonQuery(sql) != -1)
                {
                    // Change user password command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(ChangeUserPassword)} - " +
                        "Change user password command failed.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(ChangeUserPassword)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Returns 1 if successful, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int GrantSuperUser(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GrantSuperUser)} - " +
                    $"Attempting to check for login...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantSuperUser)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantSuperUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to change user to superuser...
                string sql = $"ALTER USER {login} WITH SUPERUSER;";
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    // Failed to change user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantSuperUser)} - " +
                        "Failed to change user.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GrantSuperUser)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
            }
        }
        /// <summary>
        /// Returns 1 if successful, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int DenySuperUser(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(DenySuperUser)} - " +
                    $"Attempting to check for login...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenySuperUser)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenySuperUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to change user to non-superuser...
                string sql = $"ALTER USER {login} WITH NOSUPERUSER;";
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    // Failed to change user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenySuperUser)} - " +
                        "Failed to change user.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(DenySuperUser)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
            }
        }
        /// <summary>
        /// Check if the given user is a superuser or not.
        /// Returns 1 if true, 0 if false, -1 if not found, other negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int IsSuperUser(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(IsSuperUser)} - " +
                    $"Attempting to check superuser status...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(IsSuperUser)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(IsSuperUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to check if a user is superuser...
                string sql = $"SELECT usename AS role_name, usesuper " +
                             $"FROM pg_catalog.pg_user " +
                             $"WHERE usename = '{login}';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(IsSuperUser)} - " +
                        "Failed to get users.");

                    return -2;
                }
                // We have a datatable of users.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // User not found.
                    return -1;
                }
                // If here, we have a user record to check.

                // See if we have a match to the given user.
                // Get the current user...
                var sss = ((bool)dt.Rows[0]["usesuper"]);

                return sss==true ? 1: 0;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(IsSuperUser)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns 1 if successful, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int GrantDBCreate(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                    $"Attempting to check for login...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query grant user CREATEDB...
                string sql = $"ALTER USER {login} CREATEDB;";
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    // Failed to change user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                        "Failed to change user.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
            }
        }
        /// <summary>
        /// Returns 1 if successful, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int DenyDBCreate(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                    $"Attempting to check for login...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to deny CreateDB...
                string sql = $"ALTER USER {login} NOCREATEDB;";
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    // Failed to change user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                        "Failed to change user.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
            }
        }
        /// <summary>
        /// Check if the given user has CreateDB role or not.
        /// Returns 1 if true, 0 if false, -1 if not found, other negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int HasDBCreate(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(HasDBCreate)} - " +
                    $"Attempting to check DBCreate role...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(HasDBCreate)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(HasDBCreate)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to check if a user is superuser...
                string sql = $"SELECT usename AS role_name, usecreatedb " +
                             $"FROM pg_catalog.pg_user " +
                             $"WHERE usename = '{login}';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(HasDBCreate)} - " +
                        "Failed to get users.");

                    return -2;
                }
                // We have a datatable of users.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // User not found.
                    return -1;
                }
                // If here, we have a user record to check.

                // See if we have a match to the given user.
                // Get the current user...
                var sss = ((bool)dt.Rows[0]["usecreatedb"]);

                return sss==true ? 1: 0;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(HasDBCreate)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns 1 if successful, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int GrantCreateRole(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                    $"Attempting to check for login...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query grant user CreateRole...
                string sql = $"ALTER USER {login} CREATEROLE;";
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    // Failed to change user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                        "Failed to change user.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GrantDBCreate)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
            }
        }
        /// <summary>
        /// Returns 1 if successful, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int DenyCreateRole(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                    $"Attempting to check for login...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to deny CreateRole...
                string sql = $"ALTER USER {login} NOCREATEROLE;";
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    // Failed to change user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                        "Failed to change user.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(DenyDBCreate)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
            }
        }
        /// <summary>
        /// Check if the given user has CreateRole role or not.
        /// Returns 1 if true, 0 if false, -1 if not found, other negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int HasCreateRole(string login)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(HasDBCreate)} - " +
                    $"Attempting to check CreateRole role...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(HasDBCreate)} - " +
                        $"Empty login name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(HasDBCreate)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to check if a user has CreateRole...
                string sql = $"SELECT rolcreaterole " +
                             $"FROM pg_catalog.pg_roles " +
                             $"WHERE rolname = '{login}';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(HasDBCreate)} - " +
                        "Failed to get users.");

                    return -2;
                }
                // We have a datatable of users.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // User not found.
                    return -1;
                }
                // If here, we have a user record to check.

                // See if we have a match to the given user.
                // Get the current user...
                var sss = ((bool)dt.Rows[0]["rolcreaterole"]);

                return sss==true ? 1: 0;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(HasDBCreate)} - " +
                    $"Exception occurred.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Is_Database_Present(string database)
        {
            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Is_Database_Present)} - " +
                    $"Attempting to get database names...");

                if(string.IsNullOrEmpty(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Is_Database_Present)} - " +
                        $"Empty database name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Is_Database_Present)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query for the database list...
                string sql = $"SELECT datname " +
                             $"FROM pg_database " +
                             $"WHERE datistemplate = 'false' " +
                             $"AND datname = '{database}';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get database list.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Is_Database_Present)} - " +
                        "Failed to get database list.");

                    return -2;
                }
                // We have a datatable of database list.

                // See if it contains anything.
                if (dt.Rows.Count < 1)
                {
                    // Database not found.
                    return 0;
                }
                // Database was found.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Is_Database_Present)} - " +
                    "Exception occurred while querying for database list.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }
        /// <summary>
        /// Creates a database with the given name.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Create_Database(string database)
        {
            string sql = "";

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Create_Database)} - " +
                    $"Attempting to create database...");

                if(string.IsNullOrEmpty(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Database)} - " +
                        $"Empty database name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Database)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // Database name is set.

                // See if the database doesn't already exist.
                if (this.Is_Database_Present(database) == 1)
                {
                    // The database already exists.
                    // We cannot create it again.
                    return -2;
                }

                // Formulate the sql command...
                sql = $"CREATE DATABASE \"{database}\" " +
                      $"WITH OWNER = \"postgres\" " +
                      $"ENCODING = 'UTF8' " +
                      $"CONNECTION LIMIT = -1;";

                // Execute it on the postgres instance.
                int res123 = _dal.Execute_NonQuery(sql);
                if (res123 != -1)
                {
                    // Error occurred while adding the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Database)} - " +
                        "Error occurred while adding the database.");

                    return -4;
                }

                // Check if the database is now present on the server.
                if (this.Is_Database_Present(database) != 1)
                {
                    // The database was not created successfully.
                    return -5;
                }
                // If here, the database was added.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Create_Database)} - " +
                    "Exception occurred");

                return -20;
            }
        }
        /// <summary>
        /// Drops the given database from the SQL Server instance.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="force"></param>
        /// <returns></returns>
        public int Drop_Database(string database, bool force = false)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Drop_Database)} - " +
                    $"Attempting to drop database...");

                if(string.IsNullOrEmpty(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Database)} - " +
                        $"Empty database name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Database)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // Database name is set.

                // See if the database exists...
                int res = this.Is_Database_Present(database);
                if (res == 0)
                {
                    // The database doesn't exist.
                    return 0;
                }
                else if (res < 0)
                {
                    // Failed to connect.
                    return -2;
                }
                // The database exists.
                // We will attempt to drop it.

                // Compose the sql to drop the database...
                string sql = $"DROP DATABASE IF EXISTS {database}";
                if(force)
                    sql = sql + $" WITH (FORCE);";
                else
                    sql = sql + $";";

                // Execute it on the sql server instance...
                int resdrop = _dal.Execute_NonQuery(sql);
                if (resdrop != -1)
                {
                    // Error occurred while dropping the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Database)} - " +
                        "Error occurred while dropping the database.");

                    return -4;
                }

                // Check if the database is still present on the server.
                if (this.Is_Database_Present(database) == 1)
                {
                    // The database was not deleted successfully.
                    return -5;
                }
                // If here, the database was deleted.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Drop_Database)} - " +
                    "Exception occurred");

                return -20;
            }
        }


        /// <summary>
        /// Performs a backup of the given database to the given filepath.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public int Backup_Database(string databaseName, string filePath)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Backup_Database)} - " +
                    $"Attempting to backup database to file...\r\n:" +
                    $"Database = {databaseName ?? ""};\r\n" +
                    $"BackupFile = {filePath ?? ""};");

                // Verify both givens exist...
                if(string.IsNullOrEmpty(databaseName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Backup_Database)} - " +
                        $"Empty database name.");

                    return -1;
                }
                if(string.IsNullOrEmpty(filePath))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Backup_Database)} - " +
                        $"Empty filepath.");

                    return -1;
                }

                // Verify the database exists...
                int res2 = this.Is_Database_Present(databaseName);
                if (res2 != 1)
                {
                    // Database doesn't exist.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Backup_Database)} - " +
                        "Database not found.");

                    return -1;
                }


                // ADD LOGIC TO RUN THE pg_dump command from the command line...
                // See this for the bat file usage: https://github.com/joemoceri/database-toolkit/tree/main
                return -9999;

                var process = new Process();
                var startInfo = new ProcessStartInfo();
                startInfo.FileName = Path.Combine("PostgreSQL", "postgresql-backup.bat");
                var port = 5432;

                // use pg_dump, specifying the host, port, user, database to back up, and the output path.
                // the host, port, user, and database must be an exact match with what's inside your pgpass.conf (Windows)
                startInfo.Arguments = $@"{Hostname} {port.ToString()} {this.Username} {databaseName} ""{filePath}""";
                startInfo.CreateNoWindow = true;
                startInfo.UseShellExecute = false;
                process.StartInfo = startInfo;
                process.Start();
                process.WaitForExit();
                process.Close();


                // Database was backed up.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Backup_Database)} - " +
                    "Backup finished.");

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Backup_Database)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    _dal.Disconnect();
                }
                catch (Exception e) { }
            }
        }
        /// <summary>
        /// Restores a database backup, given by filepath, to a target database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public int Restore_Database(string databaseName, string filePath)
        {
            string sql = "";

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Restore_Database)} - " +
                    $"Attempting to restore database from file...\r\n:" +
                    $"Database = {databaseName ?? ""};\r\n" +
                    $"BackupFile = {filePath ?? ""};");

                // Verify both givens exist...
                if(string.IsNullOrEmpty(databaseName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Restore_Database)} - " +
                        $"Empty database name.");

                    return -1;
                }
                if(string.IsNullOrEmpty(filePath))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Restore_Database)} - " +
                        $"Empty filepath.");

                    return -1;
                }

                // Verify the database doesn't exist...
                int res2 = this.Is_Database_Present(databaseName);
                if (res2 == 1)
                {
                    // Database doesn't exist.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Restore_Database)} - " +
                        "Database exists.");

                    return -1;
                }
                else if(res2 < 0)
                {
                    // Failed to query for database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Restore_Database)} - " +
                        "Failed to query for database.");

                    return -1;
                }

                // ADD LOGIC TO RUN THE pg_restore command from the command line...
                // See this for the bat file usage: https://github.com/joemoceri/database-toolkit/tree/main
                return -9999;

                var process = new Process();
                var startInfo = new ProcessStartInfo();
                startInfo.FileName = Path.Combine("PostgreSQL", "postgresql-restore.bat");
                var port = 5432;

                // use pg_restore, specifying the host, port, user, database to restore, and the output path.
                // the host, port, user, and database must be an exact match with what's inside your pgpass.conf (Windows)
                startInfo.Arguments = $@"{Hostname} {port.ToString()} {Username} {databaseName} ""{filePath}""";
                startInfo.CreateNoWindow = true;
                startInfo.UseShellExecute = false;
                process.StartInfo = startInfo;
                process.Start();
                process.WaitForExit();
                process.Close();

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Restore_Database)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    _dal.Disconnect();
                }
                catch (Exception e) { }
            }
        }

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="folderpath"></param>
        /// <returns></returns>
        public int Get_DataDirectory(out string folderpath)
        {
            folderpath = "";
            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_DataDirectory)} - " +
                    $"Attempting to get data foldet path...");

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_DataDirectory)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query for the file locations...
                string sql = $"SELECT name AS parmname, setting, category " +
                             $"FROM pg_settings " +
                             $"WHERE category = 'File Locations' " +
                             $"AND name = 'data_directory';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get file locations.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_DataDirectory)} - " +
                        "Failed to get file locations.");

                    return -2;
                }
                // We have a datatable of file locations.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // Data directory not found.
                    return 0;
                }
                // Database was found.

                // Get the data directory...
                folderpath = ((string?)dt.Rows[0]["setting"]) ?? "";

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_DataDirectory)} - " +
                    "Exception occurred while querying for file locations.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="folderpath"></param>
        /// <returns></returns>
        public int Get_Database_FolderPath(string databaseName, out string folderpath)
        {
            folderpath = "";
            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                    $"Attempting to get data foldet path...");

                // Verify both givens exist...
                if(string.IsNullOrEmpty(databaseName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                        $"Empty database name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Get the base data folder path...
                var respath = this.Get_DataDirectory(out var datafolderpath);
                if(respath != 1)
                {
                    // Failed to get data directory.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                        $"Failed to get data directory.");

                    return -2;

                }

                // Compose the sql query for the database oid...
                string sql = $"SELECT oid " +
                             $"from pg_database " +
                             $"where datname = '{databaseName}';";

                // Get the oid of the database...
                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get file locations.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                        "Failed to get database oid.");

                    return -2;
                }
                // We have a list of oids.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // Database oid not found.
                    return 0;
                }
                // Database oid was found.

                // Get the database oid...
                var oid = ((System.UInt32)dt.Rows[0]["oid"]);

                // Compose the database folder path...
                folderpath = System.IO.Path.Combine(datafolderpath, "base", oid.ToString());

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                    "Exception occurred while querying for database folder path.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Retrieves the list of tables for the given database.
        /// NOTE: This command must be executed on a connection with the given database, not to the system database, postgres.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="tablelist"></param>
        /// <returns></returns>
        public int Get_TableList_forDatabase(string databaseName, out List<string> tablelist)
        {
            System.Data.DataTable dt = null;
            tablelist = new List<string>();

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = databaseName;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_TableList_forDatabase)} - " +
                    $"Attempting to get table names for database {databaseName ?? ""}...");

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_Database_FolderPath)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT table_name " +
                             $"FROM information_schema.tables " +
                             $"WHERE table_schema not in ('pg_catalog', 'information_schema') " +
                             $"AND table_catalog = '{databaseName}';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get table names from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_TableList_forDatabase)} - " +
                        "Failed to get table names from the database.");

                    return -2;
                }
                // We have a datatable of table names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No tables in the database.
                    // Or, the database doesn't exist.

                    return 1;
                }
                // If here, we have tables for the database.

                // See if we have a match to the given tablename.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    string sss = r[0] + "";
                    tablelist.Add(sss);
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_TableList_forDatabase)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Gets the table size of the given table in the current database.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public (int res, long size) Get_TableSize(string tablename)
        {
            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_TableSize)} - " +
                    $"Attempting to get table size for table, {tablename ?? ""}...");

                if(string.IsNullOrEmpty(tablename))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_TableSize)} - " +
                        "Empty tablename.");

                    return (-1, 0);
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_TableSize)} - " +
                        $"Failed to connect to server.");

                    return (-1, 0);
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT pg_total_relation_size((SELECT oid FROM pg_class WHERE relname = '{tablename}'));";
                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get table size.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_TableSize)} - " +
                        $"Failed to get table size for table, {tablename ?? ""}.");

                    return (-2, 0);
                }
                // We have a table size.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // Table not found.

                    return (0, 0);
                }
                // If here, we have the table entry.

                long sss = ((long)dt.Rows[0][0]);
                return (1, sss);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_TableSize)} - " +
                    "Exception occurred");

                return (-20, 0);
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Gets the disk space used by the given database.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public (int res, long size) Get_DatabaseSize(string databaseName)
        {
            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = databaseName;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_DatabaseSize)} - " +
                    $"Attempting to get disk size for database, {databaseName ?? ""}...");

                if(string.IsNullOrEmpty(databaseName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_DatabaseSize)} - " +
                        "Empty database.");

                    return (-1, 0);
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_DatabaseSize)} - " +
                        $"Failed to connect to server.");

                    return (-1, 0);
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT pg_database_size('{databaseName}');";
                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get database size.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_DatabaseSize)} - " +
                        $"Failed to get disk size for database, {databaseName ?? ""}.");

                    return (-2, 0);
                }
                // We have a database size.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // Database not found.

                    return (0, 0);
                }
                // If here, we have the database entry.

                long sss = ((long)dt.Rows[0][0]);
                return (1, sss);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_DatabaseSize)} - " +
                    "Exception occurred");

                return (-20, 0);
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Gets the row count for each table in the database the user connects to.
        /// </summary>
        /// <param name="rowdata"></param>
        /// <returns></returns>
        public int Get_RowCount_for_Tables(out List<KeyValuePair<string, long>> rowdata)
        {
            System.Data.DataTable dt = null;
            rowdata = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_RowCount_for_Tables)} - " +
                    $"Attempting to get table row counts for database {Database}...");

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_RowCount_for_Tables)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT table_name, (SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = table_name) AS row_count " +
                             $"FROM information_schema.tables " +
                             $"WHERE table_schema = 'public';";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get row counts from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_RowCount_for_Tables)} - " +
                        "Failed to get row counts from the database.");

                    return -2;
                }
                // We have a datatable of row counts.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No tables in the database.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_RowCount_for_Tables)} - " +
                        $"Did not get any row counts for database {Database}. Database name might be wrong.");

                    return -3;
                }
                // If here, we have row counts for the database.

                // Turn them into a list.
                rowdata = new List<KeyValuePair<string, long>>();
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the table name.
                    string tablename = r["table_name"].ToString() + "";
                    long tablesize = 0;

                    // Get the table size.
                    try
                    {
                        string tempval = r["row_count"].ToString() + "";
                        tablesize = Convert.ToInt64(tempval);
                    }
                    catch (Exception e)
                    {
                        // An exception occurred while parsing in table row size data.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                            $"{_classname}:-:{nameof(Get_RowCount_for_Tables)} - " +
                            $"An exception occurred while parsing in table row size data for database {Database}.");

                        return -4;
                    }
                    KeyValuePair<string, long> vv = new KeyValuePair<string, long>(tablename, tablesize);
                    rowdata.Add(vv);
                }
                // If here, we have iterated all rows, and can return to the caller.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_RowCount_for_Tables)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Grants all privileges to a given user account for a given database.
        /// Returns 1 if found, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login"></param>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public int GrantAllforUserOnDatabase(string login, string databaseName)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = databaseName;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                    $"Attempting to grant privileges on database, {databaseName ?? ""}...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                        "Empty login.");

                    return -1;
                }
                if(string.IsNullOrEmpty(databaseName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                        "Empty database.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Ensure the user exists...
                var res1 = this.Does_Login_Exist(login);
                if(res1 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                        $"Login Not Found.");

                    return -1;
                }

                // Ensure the database exist...
                var res2 = this.Is_Database_Present(databaseName);
                if(res2 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                        $"Database Not Found.");

                    return -1;
                }
                // Both the login and database exist.


                // Compose the sql...
                string sql = $"GRANT ALL PRIVILEGES ON DATABASE '{databaseName}' TO '{login}';";
                if (_dal.Execute_NonQuery(sql) != 1)
                {
                    // Failed to get database size.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                        $"Failed to grant privileges on database, {databaseName ?? ""}.");

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GrantAllforUserOnDatabase)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Grants all privileges to a given user account for a given table.
        /// Returns 1 if found, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int GrantAllforUserOnTable(string login, string tableName)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                    $"Attempting to grant privileges on table, {tableName ?? ""}...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                        "Empty login.");

                    return -1;
                }
                if(string.IsNullOrEmpty(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                        "Empty tableName.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Ensure the user exists...
                var res1 = this.Does_Login_Exist(login);
                if(res1 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                        $"Login Not Found.");

                    return 0;
                }

                var res2 = this.DoesTableExist(tableName);
                if(res2 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                        $"Table Not Found.");

                    return 0;
                }
                // Both the login and table exist.


                // Compose the sql...
                string sql = $"GRANT ALL PRIVILEGES ON '{tableName}' TO '{login}';";
                if (_dal.Execute_NonQuery(sql) != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                        $"Failed to grant privileges on table, {tableName ?? ""}.");

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GrantAllforUserOnTable)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Sets a given set of privileges to a given user account for a given table.
        /// This is used to grant and revoke privileges for a user, depending on the bitwise enumerated privileges parameter.
        /// Returns 1 if found, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="login"></param>
        /// <param name="privileges"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int SetTablePrivilegesforUser(string login, eTablePrivileges privileges, string tableName)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                    $"Attempting to grant privilege ({privileges.ToString()}) on table, {(tableName ?? "")}...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        "Empty login.");

                    return -1;
                }
                if(string.IsNullOrEmpty(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        "Empty tableName.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Ensure the user exists...
                var res1 = this.Does_Login_Exist(login);
                if(res1 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        $"Login Not Found.");

                    return -1;
                }

                // Ensure the table exist...
                var res2 = this.DoesTableExist(tableName);
                if(res2 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        $"Table Not Found.");

                    return -1;
                }
                // Both the login and table exist.

                // We need to iterate the privilege set, and compose the necessary sql command...
                string sql = "";
                if(privileges == eTablePrivileges.NONE)
                {
                    // This is the special case of no privileges for the given user on the table.

                    // We will remove all privileges for the given user on the table.
                    sql = $"REVOKE ALL PRIVILEGES ON {tableName} FROM \"{login}\";";
                }
                else
                {
                    // The privileges parameter includes at least one privilege we need to GRANT or retain, and possibly some we need to REVOKE.
                    // To know for sure, we will need to get the current list of table privileges for the user.

                    // Get current privileges for the user...
                    var respriv = this.GetTablePrivilegesforUser(tableName, login, out var existingprivs);
                    if(respriv != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                            $"Failed to get user privileges for table.");

                        return -1;
                    }

                    // Figure out what privileges we need to add and remove...
                    var pcl = DetermineTablePrivilegeChanges(existingprivs, privileges);
                    if(pcl.Count == 0)
                    {
                        // No changes to make.
                        return 1;
                    }

                    string sqladd = "";
                    string sqlremove = "";

                    // Create the add privilege command...
                    if(pcl.Where(n=>n.isgrant == true).Count() > 0)
                    {
                        // At least one grant is needed.

                        // String together the privileges to add...
                        var pieces = String.Join(", ", pcl.Where(n => n.isgrant == true).Select(p => p.priv.ToString()).ToArray());

                        // Create the grant statement...
                        sqladd = $"GRANT {pieces} ON {tableName} TO \"{login}\";";
                    }

                    // Create the remove privilege command...
                    if(pcl.Where(n=>n.isgrant == false).Count() > 0)
                    {
                        // At least one revoke is needed.

                        // String together the privileges to revoke...
                        var pieces = String.Join(", ", pcl.Where(n => n.isgrant == false).Select(p => p.priv.ToString()).ToArray());

                        // Create the revoke statement...
                        sqlremove = $"REVOKE {pieces} ON {tableName} FROM \"{login}\";";
                    }

                    // Create a single sql command to run...
                    if(!string.IsNullOrEmpty(sqladd))
                        sql = sql + sqladd;
                    if(!string.IsNullOrEmpty(sqlremove))
                        sql = sql + sqlremove;
                }
                // We have a sql command to run.

                // Execute the composite privilege change command...
                if (_dal.Execute_NonQuery(sql) != -1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        $"Failed to update privileges for user ({(login ?? "")}) on table, {tableName ?? ""}.");

                    return -2;
                }

                // Get updated privileges for the user...
                var respriv2 = this.GetTablePrivilegesforUser(tableName, login, out var updatedprivs);
                if(respriv2 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        $"Failed to get updated user privileges for table.");

                    return -1;
                }

                // Verify privileges were updated to match what was required...
                if(updatedprivs != privileges)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                        $"Privileges were not updated.");

                    return -3;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(SetTablePrivilegesforUser)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Queries for table privileges of a given user.
        /// NOTE: The table must be in the connected database.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="login"></param>
        /// <param name="privileges"></param>
        /// <returns></returns>
        public int GetTablePrivilegesforUser(string tableName, string login, out eTablePrivileges privileges)
        {
            privileges = eTablePrivileges.NONE;

            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                    $"Attempting to query privileges on table, {(tableName ?? "")} for user ({(login ?? "")})...");

                if(string.IsNullOrEmpty(login))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                        "Empty login.");

                    return -1;
                }
                if(string.IsNullOrEmpty(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                        "Empty tableName.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Ensure the user exists...
                var res1 = this.Does_Login_Exist(login);
                if(res1 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                        $"Login Not Found.");

                    return -1;
                }

                // Ensure the table exist...
                var res2 = this.DoesTableExist(tableName);
                if(res2 != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                        $"Table Not Found.");

                    return -1;
                }
                // Both the login and table exist.

                // Query user privileges...
                string sql = $"SELECT grantor, grantee, table_catalog, table_schema, table_name, privilege_type " +
                             $"FROM information_schema.table_privileges " +
                             $"where grantee = '{login}' " +
                             $"AND table_name = '{tableName}';";
                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                        $"Failed to query privileges for user ({(login ?? "")}) on table, {tableName ?? ""}.");

                    return -2;
                }

                // We have a datatable of privileges.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No user privileges for the table.

                    privileges = eTablePrivileges.NONE;
                    return 1;
                }
                // If here, we have privileges for the table.

                // Create the bitwise enumeration...
                eTablePrivileges privs = eTablePrivileges.NONE;
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the privilege name.
                    string privname = r["privilege_type"].ToString() + "";

                    if(privname == eTablePrivileges.SELECT.ToString())
                        privs = privs | eTablePrivileges.SELECT;
                    if(privname == eTablePrivileges.INSERT.ToString())
                        privs = privs | eTablePrivileges.INSERT;
                    if(privname == eTablePrivileges.UPDATE.ToString())
                        privs = privs | eTablePrivileges.UPDATE;
                    if(privname == eTablePrivileges.DELETE.ToString())
                        privs = privs | eTablePrivileges.DELETE;
                    if(privname == eTablePrivileges.TRUNCATE.ToString())
                        privs = privs | eTablePrivileges.TRUNCATE;
                    if(privname == eTablePrivileges.REFERENCES.ToString())
                        privs = privs | eTablePrivileges.REFERENCES;
                    if(privname == eTablePrivileges.TRIGGER.ToString())
                        privs = privs | eTablePrivileges.TRIGGER;
                }
                // If here, we have iterated all rows, and can return to the caller.

                privileges = privs;
                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(GetTablePrivilegesforUser)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Checks if the given table exists in the connected database.
        /// Returns 1 if exists, 0 if not. Negatives for errors.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int DoesTableExist(string tableName)
        {
            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(DoesTableExist)} - " +
                    $"Attempting to query if table ({(tableName ?? "")}) exists...");

                if(string.IsNullOrEmpty(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DoesTableExist)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DoesTableExist)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Get the table list...
                var res2 = this.Get_TableList_forDatabase(Database, out var tl);
                if(res2 != 1 || tl == null)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DoesTableExist)} - " +
                        $"Table Not Found.");

                    return -1;
                }
                if(!tl.Contains(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(DoesTableExist)} - " +
                        $"Table Not Found.");

                    return 0;
                }
                // If here, the table was found in the connected database.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(DoesTableExist)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Creates a table in the connected database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="tabledef"></param>
        /// <returns></returns>
        public int Create_Table(TableDefinition tabledef)
        {
            string sql = "";

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            if(tabledef == null)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Create_Table)} - " +
                    $"Null table definition.");

                return -1;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Create_Table)} - " +
                    $"Attempting to create table ({(tabledef.tablename ?? "")})...");

                if(string.IsNullOrEmpty(tabledef.tablename))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Table)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Table)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // Table name is set.

                // Check if the table exists or not...
                var res2 = this.DoesTableExist(tabledef.tablename);
                if (res2 == 1)
                {
                    // Already present.
                    return 1;
                }
                if (res2 < 0)
                {
                    // Failed to query for table.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Table)} - " +
                        $"Failed to query for table.");

                    return -1;
                }
                // If here, the table doesn't yet exist.

                // Formulate the sql command...
                sql = tabledef.CreateSQLCmd();

                // Execute it on the postgres instance.
                int res123 = _dal.Execute_NonQuery(sql);
                if (res123 != -1)
                {
                    // Error occurred while adding the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Create_Table)} - " +
                        "Error occurred while adding the table.");

                    return -4;
                }

                // Check if the table is now present on the server.
                if (this.DoesTableExist(tabledef.tablename) != 1)
                {
                    // The table was not created successfully.
                    return -5;
                }
                // If here, the table was added.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Create_Table)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Drops a table from the connected database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int Drop_Table(string tableName)
        {
            string sql = "";

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Drop_Table)} - " +
                    $"Attempting to drop table ({(tableName ?? "")})...");

                if(string.IsNullOrEmpty(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Table)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Table)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Check if the table exists or not...
                var res2 = this.DoesTableExist(tableName);
                if (res2 == 0)
                {
                    // Already deleted.
                    return 1;
                }
                if (res2 < 0)
                {
                    // Failed to query for table.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Table)} - " +
                        $"Failed to query for table.");

                    return -1;
                }
                // If here, the table still exists.

                // Formulate the sql command...
                sql = $"DROP TABLE IF EXISTS {tableName};";

                // Execute it on the postgres instance.
                int res123 = _dal.Execute_NonQuery(sql);
                if (res123 != -1)
                {
                    // Error occurred while dropping the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Table)} - " +
                        "Error occurred while dropping the table.");

                    return -4;
                }

                // Check if the table is still present on the server.
                if (this.DoesTableExist(tableName) != 1)
                {
                    // The table was not dropped as expected.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Drop_Table)} - " +
                        "Table drop failed. Table is still present.");

                    return -5;
                }
                // If here, the table was dropped.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Drop_Table)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Retrieves the list of columns for the given table.
        /// NOTE: This command must be executed on a connection with the given database, not to the system database, postgres.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnlist"></param>
        /// <returns></returns>
        public int Get_ColumnList_forTable(string tableName, out List<string> columnlist)
        {
            System.Data.DataTable dt = null;
            columnlist = new List<string>();

            if (_dal == null)
            {
                _dal = new Postgres_DAL();
                _dal.Hostname = Hostname;
                _dal.Database = Database;
                _dal.Username = Username;
                _dal.Password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Get_ColumnList_forTable)} - " +
                    $"Attempting to get table names for table {tableName ?? ""}...");

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_ColumnList_forTable)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT table_catalog, table_name, column_name, ordinal_position, is_nullable, data_type, character_maximum_length " +
                             $"FROM information_schema.columns " +
                             $"WHERE table_schema = 'public' " +
                             $"AND table_name   = '{tableName}' " +
                             $"ORDER BY ordinal_position;";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get column names from the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Get_ColumnList_forTable)} - " +
                        "Failed to get column names from the table.");

                    return -2;
                }
                // We have a datatable of column names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No column in the table.
                    // Or, the table doesn't exist.

                    return 1;
                }
                // If here, we have columns for the table.

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    string sss = r["column_name"] + "";
                    columnlist.Add(sss);
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Get_ColumnList_forTable)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        ///// <summary>
        ///// Adds a user to a particular database.
        ///// Returns 1 on success, 0 if already set, negatives for errors.
        ///// </summary>
        ///// <param name="login">Account login string</param>
        ///// <param name="database">Name of database</param>
        ///// <param name="desiredroles">List of desired roles to add to user</param>
        ///// <returns></returns>
        //public int Add_User_to_Database(string login, string database, List<eDBRoles> desiredroles)
        //{
        //    // Check that the user is in the logins list already.
        //    int res = this.Does_Login_Exist(login);
        //    if (res != 1)
        //    {
        //        // Error occurred.
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //            "Login not found found."
        //                , nameof(Postgres_Tools));

        //        return -1;
        //    }
        //    // Login exists.

        //    // See if the login is tied to the desired role already.
        //    if(this.Get_DBRoles_for_Login(login, database, out var foundroles) != 1)
        //    {
        //        // Failed to get roles for the user.
        //        return -2;
        //    }
        //    // We have roles for the user.

        //    List<eDBRoles> rolestoadd = new List<eDBRoles>();
        //    // See if the desired roles are in the list.
        //    foreach(var dr in desiredroles)
        //    {
        //        if(!foundroles.Contains(dr))
        //        {
        //            // The current desired role is not in the found list.
        //            // We need to add it.
        //            rolestoadd.Add(dr);
        //        }
        //        else
        //        {
        //            // The current desired role is already in the found list.
        //        }
        //    }
        //    // At this point, we have a list of roles to add.

        //    // We will add it.
        //    if (_dal == null)
        //    {
        //        _dal = new Postgres_DAL();
        //        _dal.host = HostName;
        //        _dal.service = Service;
        //        _dal.database = database;
        //        _dal.username = Username;
        //        _dal.password = Password;
        //    }

        //    try
        //    {
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //            "Attempting to add database roles for login..."
        //                , nameof(Postgres_Tools), Database);

        //        // See if we can connect to the database.
        //        if (_dal.Test_Connection() != 1)
        //        {
        //            // Failed to connect to database.
        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Failed to connect to database."
        //                    , nameof(Postgres_Tools));

        //            return -1;
        //        }

        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //            "We can connect to the database."
        //                , nameof(Postgres_Tools));

        //        if(rolestoadd.Count == 0)
        //        {
        //            // Nothing to do.
        //            return 1;
        //        }
        //        // At least one role has to be added.

        //        // Iterate each role and add them for the user in the database...
        //        foreach (var rta in rolestoadd)
        //        {
        //            // Compose the sql query to add the database user and each needed role.
        //            string sql = "IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N'" + login + "') " +
        //                         "BEGIN " +
        //                         "    CREATE USER[" + login + "] FOR LOGIN[" + login + "] " +
        //                         "END; " +
        //                         "EXEC sp_addrolemember N'" + rta.ToString() + "', N'" + login + "'";

        //            if (_dal.Execute_NonQuery(sql) != 1)
        //            {
        //                // Failed to add database role to the sql server instance.
        //                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                    "Failed to add database role to the sql server instance."
        //                        , nameof(Postgres_Tools));

        //                return -2;
        //            }
        //        }

        //        return 1;
        //    }
        //    catch (Exception e)
        //    {
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
        //            "Exception occurred."
        //            , nameof(Postgres_Tools), Database);

        //        return -20;
        //    }
        //}

        ///// <summary>
        ///// Get Database Roles assigned to the given Login.
        ///// </summary>
        ///// <param name="login"></param>
        ///// <param name="database"></param>
        ///// <param name="roles"></param>
        ///// <returns></returns>
        //public int Get_DBRoles_for_Login(string login, string database, out List<OGA.MSSQL.eDBRoles> roles)
        //{
        //    roles = null;

        //    if (Get_DBRoles_for_Database(database, out var dbroles) != 1)
        //    {
        //        // Error occurred.
        //        return -1;
        //    }
        //    // If here, we have a list of roles for the database.

        //    roles = new List<eDBRoles>();

        //    // Sift through them for the given login.
        //    foreach (var s in dbroles)
        //    {
        //        if (s.LoginName.ToLower() == login.ToLower())
        //        {
        //            // Got a match.

        //            // See if we can recover a database role from the Groupname field.
        //            eDBRoles dbr = Recover_DatabaseRole_from_String(s.GroupName);
        //            if(dbr == eDBRoles.none)
        //            {
        //                // No database role in the current record.
        //                // Nothing to add.
        //            }
        //            else
        //            {
        //                // Database role recovered.
        //                // Add it to the list.
        //                roles.Add(dbr);
        //            }
        //        }
        //    }

        //    return 1;
        //}

        ///// <summary>
        ///// Get a list of all database roles for the database.
        ///// </summary>
        ///// <param name="database"></param>
        ///// <param name="roles"></param>
        ///// <returns></returns>
        //public int Get_DBRoles_for_Database(string database, out List<OGA.MSSQL.Model.DBRole_Assignments> roles)
        //{
        //    System.Data.DataTable dt = null;
        //    string sql = "";
        //    roles = null;

        //    // Compose the query that will pull database roles.
        //    sql = "USE MASTER " +
        //            "GO " +
        //            "BEGIN " +
        //            "DECLARE @SQLVerNo INT; " +
        //            "            SET @SQLVerNo = cast(substring(CAST(Serverproperty('ProductVersion') AS VARCHAR(50)), 0, charindex('.', CAST(Serverproperty('ProductVersion') AS VARCHAR(50)), 0)) as int); " +
        //            "            IF @SQLVerNo >= 9 " +
        //            "    IF EXISTS(SELECT TOP 1 * " +
        //            "              FROM Tempdb.sys.objects(nolock) " +
        //            "               WHERE name LIKE '#TUser%') " +
        //            "        DROP TABLE #TUser " +
        //            "ELSE " +
        //            "IF @SQLVerNo = 8 " +
        //            "BEGIN " +
        //            "    IF EXISTS(SELECT TOP 1 * " +
        //            "               FROM Tempdb.dbo.sysobjects(nolock) " +
        //            "               WHERE name LIKE '#TUser%') " +
        //            "        DROP TABLE #TUser " +
        //            "END " +
        //            "CREATE TABLE #TUser ( " +
        //            "    ServerName    varchar(256), " +
        //            "    DBName SYSNAME, " +
        //            "    [Name]        SYSNAME, " +
        //            "    GroupName SYSNAME NULL, " +
        //            "    LoginName SYSNAME NULL, " +
        //            "    default_database_name SYSNAME NULL, " +
        //            "    default_schema_name VARCHAR(256) NULL, " +
        //            "    Principal_id INT, " +
        //            "    [sid]         VARBINARY(85)) " +
        //            "IF @SQLVerNo = 8 " +
        //            "BEGIN " +
        //            "    INSERT INTO #TUser " +
        //            "	EXEC sp_MSForEachdb " +
        //            "    ' " +
        //            "     SELECT " +
        //            "	   @@SERVERNAME, " +
        //            "       '' ? '' as DBName, " +
        //            "       u.name As UserName, " +
        //            "      CASE WHEN(r.uid IS NULL) THEN ''public'' ELSE r.name END AS GroupName, " +
        //            "      l.name AS LoginName, " +
        //            "      NULL AS Default_db_Name, " +
        //            "      NULL as default_Schema_name, " +
        //            "      u.uid, " +
        //            "      u.sid " +
        //            "    FROM [?].dbo.sysUsers u " +
        //            "      LEFT JOIN ([?].dbo.sysMembers m " +
        //            "      JOIN[?].dbo.sysUsers r " +
        //            "      ON m.groupuid = r.uid) " +
        //            "       ON m.memberuid = u.uid " +
        //            "       LEFT JOIN dbo.sysLogins l " +
        //            "       ON u.sid = l.sid " +
        //            "     WHERE u.islogin = 1 OR u.isntname = 1 OR u.isntgroup = 1 " +
        //            "       /*and u.name like ''tester''*/ " +
        //            "     ORDER BY u.name " +
        //            "	' " +
        //            "END " +
        //            "ELSE " +
        //            "IF @SQLVerNo >= 9 " +
        //            "BEGIN " +
        //            "    INSERT INTO #TUser " +
        //            "	EXEC sp_MSForEachdb " +
        //            "	' " +
        //            "     SELECT " +
        //            "	   @@SERVERNAME, " +
        //            "	   ''?'', " +
        //            "       u.name, " +
        //            "       CASE WHEN (r.principal_id IS NULL) THEN ''public'' ELSE r.name END GroupName, " +
        //            "       l.name LoginName, " +
        //            "       l.default_database_name, " +
        //            "       u.default_schema_name, " +
        //            "       u.principal_id, " +
        //            "       u.sid " +
        //            "     FROM [?].sys.database_principals u " +
        //            "       LEFT JOIN ([?].sys.database_role_members m " +
        //            "       JOIN[?].sys.database_principals r " +
        //            "       ON m.role_principal_id = r.principal_id) " +
        //            "       ON m.member_principal_id = u.principal_id " +
        //            "       LEFT JOIN[?].sys.server_principals l " +
        //            "       ON u.sid = l.sid " +
        //            "     WHERE u.TYPE<> ''R'' " +
        //            "       /*and u.name like ''tester''*/ " +
        //            "     order by u.name " +
        //            "	 ' " +
        //            "END " +
        //            "SELECT* " +
        //            "FROM #TUser " +
        //            "WHERE DBName NOT IN ('master', 'msdb', 'tempdb', 'model') " +
        //            "ORDER BY DBName, " +
        //            " [name], " +
        //            " GroupName " +
        //            "DROP TABLE #TUser " +
        //            "END";

        //    // First, see if the database exists...
        //    int res = Is_Database_Present(database);
        //    if (res < 0)
        //    {
        //        // Error occurred.
        //        return -1;
        //    }
        //    else if (res == 0)
        //    {
        //        // Database does not exist.
        //        return -2;
        //    }
        //    // If here, the database exists.

        //    // Now, get the set of database roles...
        //    if (_dal == null)
        //    {
        //        _dal = new Postgres_DAL();
        //        _dal.host = HostName;
        //        _dal.service = Service;
        //        _dal.database = "master";
        //        _dal.username = Username;
        //        _dal.password = Password;
        //    }

        //    try
        //    {
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //            "Attempting to get database roles..."
        //                , nameof(Postgres_Tools), Database);

        //        // See if we can connect to the database.
        //        if (_dal.Test_Connection() != 1)
        //        {
        //            // Failed to connect to database.
        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Failed to connect to database."
        //                    , nameof(Postgres_Tools));

        //            return -1;
        //        }

        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //            "We can connect to the database."
        //                , nameof(Postgres_Tools));

        //        if (_dal.Execute_Table_Query(sql, out dt) != 1)
        //        {
        //            // Failed to get database roles from the database.
        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Failed to get database roles from the database."
        //                    , nameof(Postgres_Tools));

        //            return -2;
        //        }
        //        // We have a datatable of database roles.

        //        // See if it contains anything.
        //        if (dt.Rows.Count == 0)
        //        {
        //            // No database roles are defined.
        //            // Return an error.

        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Did not get any database roles. Something is wrong."
        //                    , nameof(Postgres_Tools), database);

        //            return -3;
        //        }
        //        // If here, we have database roles.

        //        roles = new List<OGA.MSSQL.Model.DBRole_Assignments>();

        //        // Convert each result row to a database role instance.
        //        foreach (System.Data.DataRow r in dt.Rows)
        //        {
        //            OGA.MSSQL.Model.DBRole_Assignments role = new OGA.MSSQL.Model.DBRole_Assignments();

        //            role.ServerName = r["ServerName"] + "";
        //            role.DBName = r["DBName"] + "";
        //            role.UserName = r["Name"] + "";
        //            role.GroupName = r["GroupName"] + "";
        //            role.LoginName = r["LoginName"] + "";
        //            role.Default_Database_Name = r["default_database_name"] + "";
        //            role.Default_Schema_Name = r["default_schema_name"] + "";
        //            role.Principal_ID = r["Principal_id"] + "";
        //            role.SID = r["sid"] + "";

        //            roles.Add(role);
        //        }
        //        // If here, we got a list of roles for the database.

        //        return 1;
        //    }
        //    catch (Exception e)
        //    {
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
        //            "Exception occurred"
        //            , nameof(Postgres_Tools), Database);

        //        return -20;
        //    }
        //    finally
        //    {
        //        try
        //        {
        //            dt?.Dispose();
        //        }
        //        catch (Exception) { }
        //    }
        //}



        ///// <summary>
        ///// Gets a list of column names for the given table of the database the instance is connected to.
        ///// Returns 1 for success. Negatives for errors.
        ///// </summary>
        ///// <param name="tablename"></param>
        ///// <param name="ColumnNames"></param>
        ///// <returns></returns>
        //public int Get_Columns_for_Table(string tablename, out List<string> ColumnNames)
        //{
        //    System.Data.DataTable dt = null;
        //    ColumnNames = null;

        //    if (_dal == null)
        //    {
        //        _dal = new Postgres_DAL();
        //        _dal.host = HostName;
        //        _dal.service = Service;
        //        _dal.database = Database;
        //        _dal.username = Username;
        //        _dal.password = Password;
        //    }

        //    try
        //    {
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //            "Attempting to get column names for table {2} of database {1}..."
        //                , nameof(Postgres_Tools), Database, tablename);

        //        // See if we can connect to the database.
        //        if (_dal.Test_Connection() != 1)
        //        {
        //            // Failed to connect to database.
        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Failed to connect to database."
        //                    , nameof(Postgres_Tools));

        //            return -1;
        //        }

        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //            "We can connect to the database."
        //                , nameof(Postgres_Tools));

        //        // Compose the sql query we will perform.
        //        string sql = "SELECT [name] FROM sys.columns WHERE object_id = OBJECT_ID('" + tablename + "')";

        //        if (_dal.Execute_Table_Query(sql, out dt) != 1)
        //        {
        //            // Failed to get column names from the database.
        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Failed to get column names from the database."
        //                    , nameof(Postgres_Tools));

        //            return -2;
        //        }
        //        // We have a datatable of column names.

        //        // See if it contains anything.
        //        if (dt.Rows.Count == 0)
        //        {
        //            // No columns in the table.
        //            // Or, the table doesn't exist.
        //            // Quite likely, the table name is wrong.
        //            // Return an error.

        //            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //                "Did not get any columns for table {1}. Table name might be wrong."
        //                    , nameof(Postgres_Tools), tablename);

        //            return -3;
        //        }
        //        // If here, we have columns for the table.

        //        // Turn them into a list.
        //        ColumnNames = new List<string>();
        //        foreach (System.Data.DataRow r in dt.Rows)
        //        {
        //            ColumnNames.Add(r[0] + "");
        //        }
        //        // Return what we have.

        //        return 1;
        //    }
        //    catch (Exception e)
        //    {
        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
        //            "Exception occurred"
        //            , nameof(Postgres_Tools), Database);

        //        return -20;
        //    }
        //    finally
        //    {
        //        try
        //        {
        //            dt?.Dispose();
        //        }
        //        catch (Exception) { }
        //    }
        //}

        ///// <summary>
        ///// Executes a query using SQLCMD.exe, sending the output to the desired CSV filepath.
        ///// </summary>
        ///// <param name="sqlhost"></param>
        ///// <param name="query"></param>
        ///// <param name="filepath"></param>
        ///// <returns></returns>
        //public int Execute_SQLCMD_Query_to_CSV(string query, string filepath)
        //{
        //    return Execute_SQLCMD_Query_to_CSV(query, filepath, false);
        //}
        ///// <summary>
        ///// Executes a query using SQLCMD.exe, sending the output to the desired CSV filepath.
        ///// Set using_trusted security to true if using windows user that runs SQLCMD. Set using_trusted security to false if using explicit user and password set in this instance.
        ///// </summary>
        ///// <param name="sqlhost"></param>
        ///// <param name="query"></param>
        ///// <param name="filepath"></param>
        ///// <param name="using_trusted_Security"></param>
        ///// <returns></returns>
        //public int Execute_SQLCMD_Query_to_CSV(string query, string filepath, bool using_trusted_Security)
        //{
        //    // Build a command of the form:
        //    // SQLCMD -S localhost -U <username> -P <password> -Q "SELECT * FROM [PKG_Bottle_Data].[dbo].[tbl_Color_InspData]" -s "," -o "D:\Color.csv"
        //    // Generate the output file path.

        //    // Information source:
        //    // https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2012/ms162773(v=sql.110)?redirectedfrom=MSDN

        //    string hostservice = Get_FullyQualified_SQLHostName();

        //    string authentication_argument = "";
        //    if (using_trusted_Security)
        //        authentication_argument = "-E";
        //    else
        //        authentication_argument = "-U \"" + this.Username + "\" -P \"" + this.Password + "\"";

        //    string args = "-S \"" + hostservice + "\" " +
        //                  authentication_argument + " " +
        //                  "-Q \"" + query + "\" " +
        //                  "-s \",\" " +
        //                  "-o \"" + filepath + "\"";

        //    // Attempt to get the path to the SQLCMD executable that we will leverage for exporting data.
        //    if (Locate_SQLCMDexe_in_FileSystem(out var toolpath) != 1)
        //    {
        //        // Failed to locate the SQLCMD exe path.

        //        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
        //            "Failed to locate the SQLCMD executable. It might not be installed, or the toolpath is not in the usual spots, or it's SQL Server 2018 which does not include SQLCMD.",
        //            nameof(Postgres_Tools));

        //        return -999;
        //    }
        //    // We have the SQLCMD executable path.

        //    string stdout = "";
        //    string stderr = "";
        //    int exitcode = -10;
        //    int res = OGA.Common.Process.cProcess_Helper.Run_Process(toolpath, args, ref stdout, ref stderr, ref exitcode);

        //    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
        //        "SQLCMD exeuted query with the following results:\r\n" +
        //        "sqlhost = " + hostservice + "\r\n" +
        //        "query = " + query + "\r\n" +
        //        "filepath = " + filepath + "\r\n" +
        //        "stdout = " + stdout + "\r\n" +
        //        "exitcode = " + exitcode.ToString() + "\r\n" +
        //        "stderr = " + stderr,
        //        nameof(Postgres_Tools));

        //    return 1;
        //}

        #endregion


        #region Private Methods

        //private eDBRoles Recover_DatabaseRole_from_String(string groupName)
        //{
        //    string tempstr = groupName.ToLower();

        //    // See what the given role is.
        //    if (tempstr == eDBRoles.db_accessadmin.ToString())
        //        return eDBRoles.db_accessadmin;
        //    else if (tempstr == eDBRoles.db_backupoperator.ToString())
        //        return eDBRoles.db_backupoperator;
        //    else if (tempstr == eDBRoles.db_datareader.ToString())
        //        return eDBRoles.db_datareader;
        //    else if (tempstr == eDBRoles.db_datawriter.ToString())
        //        return eDBRoles.db_datawriter;
        //    else if (tempstr == eDBRoles.db_ddladmin.ToString())
        //        return eDBRoles.db_ddladmin;
        //    else if (tempstr == eDBRoles.db_denydatareader.ToString())
        //        return eDBRoles.db_denydatareader;
        //    else if (tempstr == eDBRoles.db_denydatawriter.ToString())
        //        return eDBRoles.db_denydatawriter;
        //    else if (tempstr == eDBRoles.db_owner.ToString())
        //        return eDBRoles.db_owner;
        //    else if (tempstr == eDBRoles.db_securityadmin.ToString())
        //        return eDBRoles.db_securityadmin;
        //    else
        //        return eDBRoles.none;
        //}


        /// <summary>
        /// Determines the net changes between two given sets of table privileges.
        /// Used by logic that updates user table privileges as required.
        /// </summary>
        /// <param name="existingprivs"></param>
        /// <param name="privileges"></param>
        /// <returns></returns>
        static public List<(bool isgrant, eTablePrivileges priv)> DetermineTablePrivilegeChanges(eTablePrivileges existingprivs, eTablePrivileges privileges)
        {
            var pcl = new List<(bool isgrant, eTablePrivileges priv)>();

            // Figure out privileges to add...
            if (privileges.HasFlag(eTablePrivileges.DELETE) && !existingprivs.HasFlag(eTablePrivileges.DELETE))
                pcl.Add((true, eTablePrivileges.DELETE));
            if (privileges.HasFlag(eTablePrivileges.INSERT) && !existingprivs.HasFlag(eTablePrivileges.INSERT))
                pcl.Add((true, eTablePrivileges.INSERT));
            if (privileges.HasFlag(eTablePrivileges.REFERENCES) && !existingprivs.HasFlag(eTablePrivileges.REFERENCES))
                pcl.Add((true, eTablePrivileges.REFERENCES));
            if (privileges.HasFlag(eTablePrivileges.SELECT) && !existingprivs.HasFlag(eTablePrivileges.SELECT))
                pcl.Add((true, eTablePrivileges.SELECT));
            if (privileges.HasFlag(eTablePrivileges.TRIGGER) && !existingprivs.HasFlag(eTablePrivileges.TRIGGER))
                pcl.Add((true, eTablePrivileges.TRIGGER));
            if (privileges.HasFlag(eTablePrivileges.TRUNCATE) && !existingprivs.HasFlag(eTablePrivileges.TRUNCATE))
                pcl.Add((true, eTablePrivileges.TRUNCATE));
            if (privileges.HasFlag(eTablePrivileges.UPDATE) && !existingprivs.HasFlag(eTablePrivileges.UPDATE))
                pcl.Add((true, eTablePrivileges.UPDATE));

            // Figure out privileges to remove...
            if (!privileges.HasFlag(eTablePrivileges.DELETE) && existingprivs.HasFlag(eTablePrivileges.DELETE))
                pcl.Add((false, eTablePrivileges.DELETE));
            if (!privileges.HasFlag(eTablePrivileges.INSERT) && existingprivs.HasFlag(eTablePrivileges.INSERT))
                pcl.Add((false, eTablePrivileges.INSERT));
            if (!privileges.HasFlag(eTablePrivileges.REFERENCES) && existingprivs.HasFlag(eTablePrivileges.REFERENCES))
                pcl.Add((false, eTablePrivileges.REFERENCES));
            if (!privileges.HasFlag(eTablePrivileges.SELECT) && existingprivs.HasFlag(eTablePrivileges.SELECT))
                pcl.Add((false, eTablePrivileges.SELECT));
            if (!privileges.HasFlag(eTablePrivileges.TRIGGER) && existingprivs.HasFlag(eTablePrivileges.TRIGGER))
                pcl.Add((false, eTablePrivileges.TRIGGER));
            if (!privileges.HasFlag(eTablePrivileges.TRUNCATE) && existingprivs.HasFlag(eTablePrivileges.TRUNCATE))
                pcl.Add((false, eTablePrivileges.TRUNCATE));
            if (!privileges.HasFlag(eTablePrivileges.UPDATE) && existingprivs.HasFlag(eTablePrivileges.UPDATE))
                pcl.Add((false, eTablePrivileges.UPDATE));

            return pcl;
        }

        static public bool UserNameIsValid(string username)
        {
            return StringIsAlphaNumberandUnderscore(username);
        }

        static public bool TableNameIsValid(string username)
        {
            return StringIsAlphaNumberandUnderscore(username);
        }

        static public bool StringIsAlphaNumberandUnderscore(string val)
        {
            if (string.IsNullOrWhiteSpace(val))
            {
                return false;
            }

            Regex regex = new Regex("^[a-zA-Z0-9_]+$");
            if (regex.IsMatch(val))
            {
                return true;
            }

            return false;
        }

        #endregion
    }
}
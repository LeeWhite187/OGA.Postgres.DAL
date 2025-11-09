using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.Common.Config.structs;
using OGA.Testing.Lib;

namespace OGA.Postgres.DAL_Tests.Helpers
{
    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class ProjectTest_Base : OGA.Testing.Lib.Test_Base_abstract
    {
        protected OGA.Common.Config.structs.cPostGresDbConfig dbcreds;

        #region Private Methods

        protected string GenerateColumnName()
        {
            var name = "testcolumn" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
            return name;
        }

        protected string GenerateDatabaseName()
        {
            var name = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
            return name;
        }

        protected string GenerateTableName()
        {
            var name = "testtable" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
            return name;
        }

        protected string GenerateTestUser()
        {
            var name = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
            return name;
        }

        /// <summary>
        /// This method creates passwords of sufficient complexity to usually pass the standard Postgres password policy requirements.
        /// </summary>
        /// <returns></returns>
        protected string GenerateUserPassword()
        {
            const string alphabet =
                "abcdefghijklmnopqrstuvwxyz" +
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                "0123456789" +
                "!@#$%^&*()_+-=[]{}";

            // Loop until we have a viable password...
            while(true)
            {
                // Create a candidate password...
                //string password = Nanoid.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                string password = Nanoid.Nanoid.Generate(size: 12, alphabet: alphabet);

                // Is it viable...
                if(IsValidPostgresPassword(password))
                    return password;
            }
        }

        /// <summary>
        /// Validates a password against typical PostgreSQL 'passwordcheck' defaults.
        /// Requires: 8+ chars, at least one lowercase, one uppercase, one digit, one symbol.
        /// </summary>
        public static bool IsValidPostgresPassword(string password)
        {
            if (string.IsNullOrEmpty(password))
                return false;

            if (password.Length < 8)
                return false;

            // Must include at least one lowercase, one uppercase, one digit, and one symbol
            if (!password.Any(char.IsLower))
                return false;

            if (!password.Any(char.IsUpper))
                return false;

            if (!password.Any(char.IsDigit))
                return false;

            if (!password.Any(c => "!@#$%^&*()_+-=[]{}".Contains(c)))
                return false;

            return true;
        }


        /// <summary>
        /// Gets a tool instance that can interact with the postgres management database.
        /// </summary>
        /// <returns></returns>
        protected Postgres_Tools Get_ToolInstance_forPostgres()
        {
            var pt = new Postgres_Tools();
            pt.Username = dbcreds.User;
            pt.Hostname = dbcreds.Host;
            pt.Password = dbcreds.Password;

            return pt;
        }

        protected void GetTestDatabaseUserCreds()
        {
            var res = Get_Config_from_CentralConfig("PostGresTestAdmin", out var config);
            if (res != 1)
                throw new Exception("Failed to get database creds.");

            var cfg = Newtonsoft.Json.JsonConvert.DeserializeObject<cPostGresDbConfig>(config);
            if(cfg == null)
                throw new Exception("Failed to get database creds.");

            dbcreds = cfg;
        }

        static public int Get_Config_from_CentralConfig(string name, out string jsonstring)
        {
            jsonstring = "";
            try
            {
                // Normally, we will look to the host control service running on the host of our docker engine.
                // But if we are not running in a container, we will look to our localhost or the dev cluster.
                string origin = "";
                origin = "192.168.1.201";
                // This was set to localhost, but overridden to point to our dev cluster.
                // origin = "localhost";


                // Compose the url for central configuration...
                // Normally, this will point to the docker host DNS entry: host.docker.internal.
                // But, we will switch this out if we are running outside of a container:
                string url = $"http://{origin}:4180/api/apiv1/Config_v1/Config/" + name;

                // Get the config from the host control service...
                var res = OGA.Common.WebService.cWebService_Client_v4.Web_Request_Method(url, OGA.Common.WebService.eHttp_Verbs.GET);

                if (res.StatusCode != System.Net.HttpStatusCode.OK)
                    return -1;

                jsonstring = res.JSONResponse;
                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{nameof(ProjectTest_Base)}:-::{nameof(Get_Config_from_CentralConfig)} - " +
                    $"Exception occurred while requesting config ({name}) from central config");

                return -1;
            }
        }

        #endregion
    }
}

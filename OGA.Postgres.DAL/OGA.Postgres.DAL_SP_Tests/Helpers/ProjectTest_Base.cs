using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.Common.Config.structs;
using OGA.Postgres;
using OGA.Testing.Lib;
using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.MSSQL.DAL_Tests.Helpers
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

        protected string GenerateUserPassword()
        {
            var name = Nanoid.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
            return name;
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
            pt.Database = "postgres";

            return pt;
        }

        protected Postgres_Tools Get_ToolInstance_forDatabase(string database)
        {
            var pt = new Postgres_Tools();
            pt.Username = dbcreds.User;
            pt.Hostname = dbcreds.Host;
            pt.Password = dbcreds.Password;
            pt.Database = database;

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

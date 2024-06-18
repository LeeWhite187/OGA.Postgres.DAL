using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.Postgres;
using OGA.SharedKernel.Process;
using OGA.SharedKernel;
using OGA.Testing.Lib;
using System;
using System.Collections.Generic;
using System.Web;
using OGA.Common.Config.structs;
using NanoidDotNet;
using System.Threading.Tasks;
using OGA.Postgres.DAL;
using System.Linq;

namespace OGA.Postgres_Tests
{
    /*  Unit Tests for PostgreSQL DAL.

        //  Test_1_1_1  Verify that we can connect to a postgres database with npgsql.
        //  Test_1_1_2  Verify that connection fails to the test postgres database with the bad admin creds.

        //  Test_1_2_1  Verify we can query for the owner of a database.
        //  Test_1_2_2  Verify we can change the owner of a database.

        //  Test_1_3_1  Verify we can get the primary key column data for a table.
     
        //  Test_1_8_1  Verify that we can create a database whose name doesn't already exist.
        //  Test_1_8_2  Verify that we cannot create a database whose name already exists.
        //  Test_1_8_3  Verify that we can verify if a database exists.
        //  Test_1_8_4  Verify that we can delete a database that exists.
        //  Test_1_8_5  Verify that we cannot delete a database with an unknown name.
        //  Test_1_8_6  Verify that a user without CreateDB is not allowed to create a database.

        //  Test_1_9_1  Verify that we can get the folder path of the data folder.
        //  Test_1_9_2  Verify that we can get the folder path of a database.

        //  Test_1_10_1  Verify that we can get a list of tables for a given database.

        //  Test_1_11_1  Verify that we can get a list of databases on the PostgreSQL host.

     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class PostgresTests : OGA.Testing.Lib.Test_Base_abstract
    {
        protected OGA.Common.Config.structs.cPostGresDbConfig dbcreds;

        #region Setup

        /// <summary>
        /// This will perform any test setup before the first class tests start.
        /// This exists, because MSTest won't call the class setup method in a base class.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test class setup method of the base.
        /// </summary>
        [ClassInitialize]
        static public void TestClass_Setup(TestContext context)
        {
            TestClassBase_Setup(context);
        }
        /// <summary>
        /// This will cleanup resources after all class tests have completed.
        /// This exists, because MSTest won't call the class cleanup method in a base class.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test class cleanup method of the base.
        /// </summary>
        [ClassCleanup]
        static public void TestClass_Cleanup()
        {
            TestClassBase_Cleanup();
        }

        /// <summary>
        /// Called before each test runs.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test setup method of the base.
        /// </summary>
        [TestInitialize]
        override public void Setup()
        {
            //// Push the TestContext instance that we received at the start of the current test, into the common property of the test base class...
            //Test_Base.TestContext = TestContext;

            base.Setup();

            // Runs before each test. (Optional)

            // Retrieve database server creds...
            this.GetTestDatabaseUserCreds();
        }

        /// <summary>
        /// Called after each test runs.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test cleanup method of the base.
        /// </summary>
        [TestCleanup]
        override public void TearDown()
        {
            // Runs after each test. (Optional)

            base.TearDown();
        }

        #endregion


        //  Test_1_1_1  Verify that we can connect to the test postgres database with the test admin creds.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            Postgres_DAL dal = null;

            try
            {
                dal = new Postgres_DAL();
                dal.Username = dbcreds.User;
                dal.Hostname = dbcreds.Host;
                dal.Password = dbcreds.Password;
                dal.Database = dbcreds.Database;

                var res = dal.Test_Connection();
                if (res != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                dal?.Dispose();
            }
        }

        //  Test_1_1_2  Verify that connection fails to the test postgres database with the bad admin creds.
        [TestMethod]
        public async Task Test_1_1_2()
        {
            Postgres_DAL dal = null;

            try
            {
                dal = new Postgres_DAL();
                dal.Username = dbcreds.User;
                dal.Hostname = dbcreds.Host;
                dal.Password = dbcreds.Password + "f";
                dal.Database = dbcreds.Database;

                var res = dal.Test_Connection();
                if (res != -1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                dal?.Dispose();
            }
        }


        //  Test_1_2_1  Verify we can query for the owner of a database.
        [TestMethod]
        public async Task Test_1_2_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");


                // Get the database owner...
                var reso = pt.GetDatabaseOwner(dbname, out var ownername);
                if(reso != 1)
                    Assert.Fail("Wrong Value");

                // Verify the owner is our user that created it...
                if (ownername != dbcreds.User)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_2_2  Verify we can change the owner of a database.
        [TestMethod]
        public async Task Test_1_2_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");


                // Get the database owner...
                var reso = pt.GetDatabaseOwner(dbname, out var ownername);
                if(reso != 1)
                    Assert.Fail("Wrong Value");

                // Verify the owner is our user that created it...
                if (ownername != dbcreds.User)
                    Assert.Fail("Wrong Value");


                // Create a second database user...
                string mortaluser1 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Transfer ownership to the second user...
                var reschg = pt.ChangeDatabaseOwner(dbname, mortaluser1);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Verify the database owner was changed...
                var resver = pt.GetDatabaseOwner(dbname, out var actualowner);
                if(resver != 1)
                    Assert.Fail("Wrong Value");
                if(actualowner != mortaluser1)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_3_1  Verify we can get the primary key column data for a table.
        [TestMethod]
        public async Task Test_1_3_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");


                // Create a test table in our test database that has a primary key...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Swap our connection to the created database...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbname;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the new database...
                    var res5 = pt.TestConnection();
                    if(res5 != 1)
                        Assert.Fail("Wrong Value");

                    // Create the table definition...
                    var tch = new TableDefinition(tblname, pt.Username);
                    tch.Add_Pk_Column("Id", Postgres.DAL.Model.ePkColTypes.integer);
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Query for the primary keys of the table...
                var respk = pt.Get_PrimaryKeyConstraints_forTable(tblname, out var pklist);
                if(respk != 1 || pklist == null)
                    Assert.Fail("Wrong Value");

                // Verify we found the primary key we created...
                if(pklist.Count != 1)
                    Assert.Fail("Wrong Value");
                var pkc = pklist.FirstOrDefault(n => n.key_column == "Id");
                if(pkc == null)
                    Assert.Fail("Wrong Value");
                if(pkc.table_name != tblname)
                    Assert.Fail("Wrong Value");


                // To drop the database, we must switch back to the postgres database...
                {
                    // Swap our connection back to the catalog...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbcreds.Database;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the postgres database...
                    var res6a = pt.TestConnection();
                    if(res6a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Delete the database...
                var res8 = pt.Drop_Database(dbname, true);
                if(res8 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res9 = pt.Is_Database_Present(dbname);
                if(res9 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_8_1  Verify that we can create a database whose name doesn't already exist.
        [TestMethod]
        public async Task Test_1_8_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_8_2  Verify that we cannot create a database whose name already exists.
        [TestMethod]
        public async Task Test_1_8_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");


                // Attempt to create the database again...
                var res2a = pt.Create_Database(dbname);
                if(res2a != -2)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_8_3  Verify that we can verify if a database exists.
        [TestMethod]
        public async Task Test_1_8_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_8_4  Verify that we can delete a database that exists.
        [TestMethod]
        public async Task Test_1_8_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Is_Database_Present(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_8_5  Verify that we cannot delete a database with an unknown name.
        [TestMethod]
        public async Task Test_1_8_5()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Check that the database doesn't exist...
                var res1 = pt.Is_Database_Present(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_8_6  Verify that a user without CreateDB is not allowed to create a database.
        [TestMethod]
        public async Task Test_1_8_6()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;


                // Create a test user...
                string mortaluser1 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");


                // Create the test database name...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");


                // Have test user 1 attempt to create the test database...
                {
                    // Open a connection as test user 1...
                    var pt1 = new Postgres_Tools();
                    pt1.Hostname = dbcreds.Host;
                    pt1.Database = dbcreds.Database;
                    pt1.Username = mortaluser1;
                    pt1.Password = mortaluser1_password;

                    // Attempt to create the test database...
                    var res1a = pt1.Create_Database(dbname);
                    if(res1a != -4)
                        Assert.Fail("Wrong Value");

                    pt1.Dispose();
                }

                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 0)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Is_Database_Present(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_9_1  Verify that we can get the folder path of the data folder.
        [TestMethod]
        public async Task Test_1_9_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;


                // Get the data folder path...
                var res = pt.Get_DataDirectory(out var folderpath);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                if(folderpath != "E:/PostGreSQL_Data")
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_9_2  Verify that we can get the folder path of a database.
        [TestMethod]
        public async Task Test_1_9_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;


                // Get the data folder path...
                var res = pt.Get_Database_FolderPath("postgres", out var folderpath);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                string normalizedpath = "E:/PostGreSQL_Data/base/13754".Replace("/", "\\");

                if(folderpath.Replace("/", "\\") != normalizedpath)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_10_1  Verify that we can get a list of tables for a given database.
        [TestMethod]
        public async Task Test_1_10_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = "dbProjectControls"; //dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;


                // Get the data folder path...
                var res = pt.Get_TableList_forDatabase("dbProjectControls", out var tablelist);
                if(res != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_11_1  Verify that we can get a list of databases on the PostgreSQL host.
        [TestMethod]
        public async Task Test_1_11_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = "postgres";
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Get a list of databases on the host...
                var res = pt.Get_DatabaseList(out var dblist);
                if(res != 1 || dblist == null)
                    Assert.Fail("Wrong Value");

                // Verify the list contains the catalog...
                if(!dblist.Contains("postgres"))
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        #region Private Methods

        private void GetTestDatabaseUserCreds()
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
                    $"{nameof(PostgresTests)}:-::{nameof(Get_Config_from_CentralConfig)} - " +
                    $"Exception occurred while requesting config ({name}) from central config");

                return -1;
            }
        }

        #endregion
    }
}

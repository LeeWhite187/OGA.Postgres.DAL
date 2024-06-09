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
using OGA.Postgres.DAL;
using System.Threading.Tasks;
using System.Linq;

namespace OGA.Postgres_Tests
{
    /*  Unit Tests for PostgreSQL Tools class.
        This set of tests exercise the method, Get_ColumnList_forTable().

        //  Test_1_1_1  Verify that we can query column names for a table.
        //  Test_1_1_2  Verify that we can query column info for a table.

     
     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class TableColumnMgmt_Tests : OGA.Testing.Lib.Test_Base_abstract
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

        
        [TestMethod]
        public async Task Test_1_1_0()
        {
            string tablename = "tbl_Icons";
            var tch = new TableDefinition(tablename, "postgres");

            var res1 = tch.Add_Pk_Column("Id", Postgres.DAL.Model.ePkColTypes.integer);
            if (res1 != 1)
                Assert.Fail("Wrong Value");

            var res2 = tch.Add_String_Column("IconName", 50, true);
            if (res2 != 1)
                Assert.Fail("Wrong Value");

            var res3 = tch.Add_Numeric_Column("Height", Postgres.DAL.Model.eNumericColTypes.integer, true);
            if (res3 != 1)
                Assert.Fail("Wrong Value");

            var res4 = tch.Add_Numeric_Column("Width", Postgres.DAL.Model.eNumericColTypes.integer, true);
            if (res4 != 1)
                Assert.Fail("Wrong Value");

            var res5 = tch.Add_String_Column("Path", 255, true);
            if (res5 != 1)
                Assert.Fail("Wrong Value");


            var sql = tch.CreateSQLCmd();

            int x = 0;
        }


        //  Test_1_1_1  Verify that we can query column names for a table.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we can test with...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col1 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col2 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col3 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col4 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Create a test database...
                    var res1 = pt.Create_Database(dbname);
                    if(res1 != 1)
                        Assert.Fail("Wrong Value");

                    // Swap our connection to the created database...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbname;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the new database...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Create the table definition...
                    var tch = new TableDefinition(tblname, pt.Username);
                    tch.Add_Pk_Column("Id", Postgres.DAL.Model.ePkColTypes.integer);
                    tch.Add_Guid_Column(col1, false);
                    tch.Add_UTCDateTime_Column(col2, false);
                    tch.Add_Numeric_Column(col3, Postgres.DAL.Model.eNumericColTypes.bigint, false);
                    tch.Add_String_Column(col4, 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }


                // Query for column names of the table...
                var res4 = pt.Get_ColumnList_forTable(tblname, out var collist);
                if(res4 != 1 || collist == null || collist.Count == 0)
                    Assert.Fail("Wrong Value");


                // Verify the list has all of our column names...
                if(!collist.Contains("Id"))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col1))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col2))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col3))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col4))
                    Assert.Fail("Wrong Value");


                // To drop the database, we must switch back to the postgres database...
                {
                    // Swap our connection to the created database...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbcreds.Database;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the postgres databaes...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Delete the database...
                var res7 = pt.Drop_Database(dbname, true);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res8 = pt.Is_Database_Present(dbname);
                if(res8 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_2  Verify that we can query column info for a table.
        [TestMethod]
        public async Task Test_1_1_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we can test with...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col1 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col2 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col3 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col4 = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Create a test database...
                    var res1 = pt.Create_Database(dbname);
                    if(res1 != 1)
                        Assert.Fail("Wrong Value");

                    // Swap our connection to the created database...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbname;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the new database...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Create the table definition...
                    var tch = new TableDefinition(tblname, pt.Username);
                    tch.Add_Pk_Column("Id", Postgres.DAL.Model.ePkColTypes.integer);
                    tch.Add_Guid_Column(col1, false);
                    tch.Add_UTCDateTime_Column(col2, true);
                    tch.Add_Numeric_Column(col3, Postgres.DAL.Model.eNumericColTypes.bigint, true);
                    tch.Add_String_Column(col4, 50, true);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }


                // Query for column info of the table...
                var res4 = pt.Get_ColumnInfo_forTable(tblname, out var coldata);
                if(res4 != 1 || coldata == null || coldata.Count == 0)
                    Assert.Fail("Wrong Value");


                // Verify the list has the correct column data...
                var c1 = coldata.FirstOrDefault(m => m.name == "Id");
                if(c1 == null)
                    Assert.Fail("Wrong Value");
                if(c1.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c1.dataType != "integer")
                    Assert.Fail("Wrong Value");
                if(c1.isNullable != false)
                    Assert.Fail("Wrong Value");
                if(c1.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c1.ordinal != 1)
                    Assert.Fail("Wrong Value");

                var c2 = coldata.FirstOrDefault(m => m.name == col1);
                if(c2 == null)
                    Assert.Fail("Wrong Value");
                if(c2.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c2.dataType != "uuid")
                    Assert.Fail("Wrong Value");
                if(c2.isNullable != false)
                    Assert.Fail("Wrong Value");
                if(c2.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c2.ordinal != 2)
                    Assert.Fail("Wrong Value");

                var c3 = coldata.FirstOrDefault(m => m.name == col2);
                if(c3 == null)
                    Assert.Fail("Wrong Value");
                if(c3.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c3.dataType != "timestamp with time zone")
                    Assert.Fail("Wrong Value");
                if(c3.isNullable != true)
                    Assert.Fail("Wrong Value");
                if(c3.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c3.ordinal != 3)
                    Assert.Fail("Wrong Value");

                var c4 = coldata.FirstOrDefault(m => m.name == col3);
                if(c4 == null)
                    Assert.Fail("Wrong Value");
                if(c4.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c4.dataType != "bigint")
                    Assert.Fail("Wrong Value");
                if(c4.isNullable != true)
                    Assert.Fail("Wrong Value");
                if(c4.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c4.ordinal != 4)
                    Assert.Fail("Wrong Value");

                var c5 = coldata.FirstOrDefault(m => m.name == col4);
                if(c5 == null)
                    Assert.Fail("Wrong Value");
                if(c5.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c5.dataType != "character varying")
                    Assert.Fail("Wrong Value");
                if(c5.isNullable != true)
                    Assert.Fail("Wrong Value");
                if(c5.maxlength != 50)
                    Assert.Fail("Wrong Value");
                if(c5.ordinal != 5)
                    Assert.Fail("Wrong Value");


                // To drop the database, we must switch back to the postgres database...
                {
                    // Swap our connection to the created database...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbcreds.Database;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the postgres databaes...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Delete the database...
                var res7 = pt.Drop_Database(dbname, true);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res8 = pt.Is_Database_Present(dbname);
                if(res8 != 0)
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

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
using Mono.Unix.Native;
using OGA.Postgres.CreateVerify;
using OGA.Postgres.DAL.Model;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Npgsql.Replication;

namespace OGA.Postgres_Tests
{
    /*  Use Case Tests for the Database Layout Tool Class.

        //  Test_1_1_0  This use-case covers the need to catalog all databases on a host, as layouts.
        //              Create database layouts from all live databases on the test server, and store them in json files.
        //  Test_1_1_1  This use-case covers the need to create an offline layout file, for a live database.
        //              Specifically, we create a database layout from a live database, and store it in a json file.
        //  Test_1_1_2  This use-case covers the need to create a live database from an offline layout file.
        //              Specifically, we load a database layout from a json file, and create a live database from it.
        //  Test_1_1_3  This use-case covers the need to verify that a live database matches an offline layout file.
        //              This is a function that services may perform on startup, to ensure the live database, is compatible.
        //              Specifically, we load a database layout from a json file, and reconcile it against a live database.

     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class DbLayout_UseCase_Tests : OGA.Testing.Lib.Test_Base_abstract
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


        #region Tests

        //  Test_1_1_0  This use-case covers the need to catalog layouts for all databases on a host.
        //              Create database layouts from all live databases on the test server, and store them in json files.
        [TestMethod]
        public async Task Test_1_1_0()
        {
            try
            {
                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Get a list of all databases on the PostgreSQL host...
                var res = dlt.GetLayouts_forAllHostDatabases();
                if(res.res != 1 || res.layouts == null)
                    Assert.Fail("Wrong Value");

                // Create json layout files from each database on the host...
                foreach (var l in res.layouts)
                {
                    // Convert to json, and store in the filesystem...
                    var jsonlayout = Newtonsoft.Json.JsonConvert.SerializeObject(l, Newtonsoft.Json.Formatting.Indented);

                    // Create a filepath for the json file...
                    var filename = $"{l.name}-{DateTime.Now.ToString("yyyMMdd-HHmmss")}.json";
                    var filepath = System.IO.Path.Combine(_testfolder, filename);

                    // Write out the layout json...
                    System.IO.File.WriteAllText(filepath, jsonlayout);
                }

                int x = 0;
            }
            finally
            {
            }
        }

        //  Test_1_1_1  This use-case covers the need to create an offline layout file, for a live database.
        //              Specifically, we create a database layout from a live database, and store it in a json file.
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

                // Create a live test database, that covers all datatypes...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Create a live database, via layout...
                DbLayout_Database layout = this.CreateLayout_UsingAllTypes();
                layout.name = dbname;

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Attempt to create the live database from the generated layout...
                var res1 = dlt.Create_Database_fromLayout(layout);
                if(res1.res != 1)
                    Assert.Fail("Wrong Value");


                // Create a layout for the given database...
                var res3 = dlt.CreateLayout_fromDatabase(layout.name);
                if(res3.res != 1 || res3.layout == null)
                    Assert.Fail("Wrong Value");

                // Convert to json, and store in the filesystem...
                var jsonlayout = Newtonsoft.Json.JsonConvert.SerializeObject(res3.layout, Newtonsoft.Json.Formatting.Indented);

                // Create a filepath for the json file...
                var filename = $"{layout.name}-{DateTime.Now.ToString("yyyMMdd-HHmmss")}.json";
                var filepath = System.IO.Path.Combine(_testfolder, filename);

                // Write out the layout json...
                System.IO.File.WriteAllText(filepath, jsonlayout);


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

        //  Test_1_1_2  This use-case covers the need to create a live database from an offline layout file.
        //              Specifically, we load a database layout from a json file, and create a live database from it.
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

                // Create a database layout, that covers all datatypes...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                DbLayout_Database layout = this.CreateLayout_UsingAllTypes();
                layout.name = dbname;


                // Simulate storing it to json...
                var jsonstring = Newtonsoft.Json.JsonConvert.SerializeObject(layout, Newtonsoft.Json.Formatting.Indented);


                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Attempt to create the live database from the generated layout...
                var res1 = dlt.Create_Database_fromLayout(layout);
                if(res1.res != 1)
                    Assert.Fail("Wrong Value");


                // Now, we need to verify that the created database matches the layout we created it from...

                // Create layout from live database...
                var res2 = dlt.CreateLayout_fromDatabase(dbname);
                if(res2.res != 1 || res2.layout == null)
                    Assert.Fail("Wrong Value");


                // Verify created layout is a match to what we generated the live database from...
                var res3 = DbLayout_Database.CompareLayouts(layout, res2.layout);
                if(res3 != 1)
                    Assert.Fail("Layout Mismatch");


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

        //  Test_1_1_3  This use-case covers the need to verify that a live database matches an offline layout file.
        //              This is a function that services may perform on startup, to ensure the live database, is compatible.
        //              Specifically, we load a database layout from a json file, and reconcile it against a live database.
        [TestMethod]
        public async Task Test_1_1_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a database layout, that covers all datatypes...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                DbLayout_Database layout = this.CreateLayout_UsingAllTypes();
                layout.name = dbname;


                // Simulate storing it to json...
                var jsonstring = Newtonsoft.Json.JsonConvert.SerializeObject(layout, Newtonsoft.Json.Formatting.Indented);


                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Attempt to create the live database from the generated layout...
                var res1 = dlt.Create_Database_fromLayout(layout);
                if(res1.res != 1)
                    Assert.Fail("Wrong Value");


                // Now, we need to verify that the created database matches the layout we created it from...

                // Verify live database against layout...
                var res2 = dlt.Verify_Database_Layout(layout);
                if(res2.res != 1)
                    Assert.Fail("Wrong Value");

                // Verify no differences were found...
                if(res2.errs != null && res2.errs.Count != 0)
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

        #endregion
        

        #region Private Methods

        /// <summary>
        /// Creates a Database layout instance that uses all known datatypes.
        /// </summary>
        /// <returns></returns>
        private DbLayout_Database CreateLayout_UsingAllTypes()
        {
            // Create a live database, via layout...
            DbLayout_Database layout = new DbLayout_Database();
            layout.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
            layout.owner = "postgres";

            // To exercise all types, we will need eight tables, to see all combinations of primary keys and identity behaviors.

            // Add table 1...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;

                // Add primary key of type, uuid...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, in a NOT IsNullable state...
                this.AddAllDatatypes_toTableLayout(tbl, false, 2);

                layout.tables.Add(tbl);
            }
            // Add table 2...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 2;

                // Add primary key of type, varchar...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_varchar;
                col1.maxlength = 74;
                col1.isNullable = false;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                // Add primary key of type, varchar...
                this.AddAllDatatypes_toTableLayout(tbl, false, 2);
                
                layout.tables.Add(tbl);
            }
            // Add table 3...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 3;

                // Add primary key of type, bigint, as generatealways identity...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_bigint;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = true;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.GenerateAlways;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, so there's presence for any unanticipated edge-case...
                this.AddAllDatatypes_toTableLayout(tbl, true, 2);

                layout.tables.Add(tbl);
            }
            // Add table 4...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 4;

                // Add primary key of type, int, as generatealways identity...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = true;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.GenerateAlways;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, so there's presence for any unanticipated edge-case...
                this.AddAllDatatypes_toTableLayout(tbl, true, 2);

                layout.tables.Add(tbl);
            }
            // Add table 5...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 5;

                // Add primary key of type, bigint, as generatedefault identity...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_bigint;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = true;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.GenerateByDefault;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, so there's presence for any unanticipated edge-case...
                this.AddAllDatatypes_toTableLayout(tbl, true, 2);

                layout.tables.Add(tbl);
            }
            // Add table 6...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 6;

                // Add primary key of type, int, as generatedefault identity...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = true;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.GenerateByDefault;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, so there's presence for any unanticipated edge-case...
                this.AddAllDatatypes_toTableLayout(tbl, true, 2);

                layout.tables.Add(tbl);
            }
            // Add table 7...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 7;

                // Add primary key of type, bigint, as NO identity...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_bigint;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, so there's presence for any unanticipated edge-case...
                this.AddAllDatatypes_toTableLayout(tbl, true, 2);

                layout.tables.Add(tbl);
            }
            // Add table 8...
            {
                DbLayout_Table tbl = new DbLayout_Table();
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 8;

                // Add primary key of type, int, as NO identity...
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.maxlength = null;
                col1.isNullable = false;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                // Add columns of each non-key type, so there's presence for any unanticipated edge-case...
                this.AddAllDatatypes_toTableLayout(tbl, true, 2);

                layout.tables.Add(tbl);
            }

            return layout;
        }

        /// <summary>
        /// Used by the the CreateLayout method chain, to one of each non-key table to the given layout.
        /// Returns the last used ordinal.
        /// </summary>
        /// <param name="tbl"></param>
        /// <param name="isnullable"></param>
        /// <param name="lastusedordinal"></param>
        private int AddAllDatatypes_toTableLayout(DbLayout_Table tbl, bool isnullable, int lastusedordinal)
        {
            if (lastusedordinal < 1)
                lastusedordinal = 1;

            // Add datetime types...
            {
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col1.ordinal = lastusedordinal;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestamp;
                col1.maxlength = null;
                col1.isNullable = isnullable;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                var col2 = new DbLayout_Column();
                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col2.ordinal = lastusedordinal;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                col2.maxlength = null;
                col2.isNullable = isnullable;
                col2.isIdentity = false;
                col2.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col2);
            }

            // Add string types...
            {
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col1.ordinal = lastusedordinal;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                col1.maxlength = 47;
                col1.isNullable = isnullable;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                var col2 = new DbLayout_Column();
                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col2.ordinal = lastusedordinal;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                col2.maxlength = 0;
                col2.isNullable = isnullable;
                col2.isIdentity = false;
                col2.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col2);
            }

            // Add bool type...
            {
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col1.ordinal = lastusedordinal;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.boolean;
                col1.maxlength = 0;
                col1.isNullable = isnullable;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);
            }

            // Add uuid type...
            {
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col1.ordinal = lastusedordinal;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.uuid;
                col1.maxlength = 0;
                col1.isNullable = isnullable;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);
            }

            // Add numeric datatypes...
            {
                var col1 = new DbLayout_Column();
                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col1.ordinal = lastusedordinal;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                col1.maxlength = 0;
                col1.isNullable = isnullable;
                col1.isIdentity = false;
                col1.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col1);

                var col2 = new DbLayout_Column();
                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col2.ordinal = lastusedordinal;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.double_precision;
                col2.maxlength = 0;
                col2.isNullable = isnullable;
                col2.isIdentity = false;
                col2.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col2);

                var col3 = new DbLayout_Column();
                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col3.ordinal = lastusedordinal;
                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col3.maxlength = 0;
                col3.isNullable = isnullable;
                col3.isIdentity = false;
                col3.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col3);

                var col4 = new DbLayout_Column();
                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col4.ordinal = lastusedordinal;
                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.numeric;
                col4.maxlength = 0;
                col4.isNullable = isnullable;
                col4.isIdentity = false;
                col4.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col4);

                var col5 = new DbLayout_Column();
                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet: "abcdefghijklmnopqrstuvwxyz01234567890");
                lastusedordinal++;
                col5.ordinal = lastusedordinal;
                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.real;
                col5.maxlength = 0;
                col5.isNullable = isnullable;
                col5.isIdentity = false;
                col5.identityBehavior = Postgres.DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                tbl.columns.Add(col5);
            }

            return lastusedordinal;
        }

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

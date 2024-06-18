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

namespace OGA.Postgres_Tests
{
    /*  Unit Tests for the Database Layout Tool Class.

        //  Test_1_1_1  Verify that we can test connection to a postgres database.

        //  Test_1_2_1  Ensure that the Verify_Database_Layout method call returns error if the given layout is null.
        //  Test_1_2_2  Ensure that the Verify_Database_Layout method call returns error if the given layout has multiple primary keys in a table.
        //  Test_1_2_3  Ensure that the Verify_Database_Layout method call returns error if the given layout has multiple tables with same name.
        //  Test_1_2_4  Ensure that the Verify_Database_Layout method call returns error if the given layout has a bad database name.
        //  Test_1_2_5  Ensure that the Verify_Database_Layout method call returns error if the given layout has a bad table name.
        //  Test_1_2_6  Ensure that the Verify_Database_Layout method call returns error if the given layout has a bad column name.
        //  Test_1_2_7  Ensure that the Verify_Database_Layout method call returns error if the given layout has multiple columns in a table, with same name.

        //  Test_1_3_1  Verify that the Verify_Database_Layout method call returns error if the layout's database doesn't exist.
        //  Test_1_3_2  Verify that the Verify_Database_Layout method call returns error for a layout database owner that doesn't exist.
        //  Test_1_3_3  Verify that the Verify_Database_Layout method call returns error for a layout database owner that is incorrect.
        //  Test_1_3_4  Verify that the Verify_Database_Layout method call returns error for a layout database table that doesn't exist.

        //  Test_1_4_0  Verify that the Verify_Database_Layout method call returns success for a layout that matches a live table with columns.
        //  Test_1_4_1  Verify that the Verify_Database_Layout method call returns error for a layout table column that doesn't exist in the live table.
        //  Test_1_4_2  Verify that the Verify_Database_Layout method call returns error for a live table column that doesn't exist in the layout table.
        //  Test_1_4_3  Verify that the Verify_Database_Layout method call returns error for a table column whose type is different in the live table.
        //  Test_1_4_3b  Verify that the Verify_Database_Layout method call returns success for a table column whose type is varchar.
        //  Test_1_4_4  Verify that the Verify_Database_Layout method call returns error for a varchar table column with a different max length in the live table.
        //  Test_1_4_5  Verify that the Verify_Database_Layout method call returns error for a live table column with a different isnullable state than the layout.
        //  Test_1_4_6  Verify that the Verify_Database_Layout method call returns error for a live table column that should be a primary key, but is not.
        //  Test_1_4_7  Verify that the Verify_Database_Layout method call returns error for a live table column that should NOT be a primary key, but is.

        //  Test_1_5_1  Verify that we can create a database with tables and columns, and a layout that verifies it.
        //              Then, verify that we can serialize the layout, deserialize it, and successfully verify the layout.

        //  Test_1_5_2  Verify that we can create a database with nothing in it, and a layout with tables and columns, and the verification call, reports all the differences.

        //  Test_1_6_1  Verify that an error results if we attempt to create create a live database from a database layout, but the database already exists.
        //  Test_1_6_2  Verify that we can create a live database from a database layout.
        //  Test_1_6_3  Verify that we can verify a database layout against a live database.
        //  Test_1_6_4  Verify that we can create a database layout from a live database.
        //  Test_1_6_5  Verify that we can use a generated database layout, to create an identical live database.

     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class DatabaseLayout_Tests : OGA.Testing.Lib.Test_Base_abstract
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

        //  Test_1_1_1  Verify that we can test connection to a postgres database.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            try
            {
                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                var res1 = dlt.TestConnection();
                if(res1 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_1  Ensure that the Verify_Database_Layout method call returns error if the given layout is null.
        [TestMethod]
        public async Task Test_1_2_1()
        {
            try
            {
                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                DbLayout_Database layout = null;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Layout)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != "")
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_2  Ensure that the Verify_Database_Layout method call returns error if the given layout has multiple primary keys in a table.
        [TestMethod]
        public async Task Test_1_2_2()
        {
            try
            {

                // Create a test database...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout with a table with multiple primary keys...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;
                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_bigint;
                col1.isNullable = false;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Table)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != tbl.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrText != "Multiple Primary Key Columns")
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ParentName != layout.name)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_3  Ensure that the Verify_Database_Layout method call returns error if the given layout has multiple tables with same name.
        [TestMethod]
        public async Task Test_1_2_3()
        {
            try
            {

                // Create layout with a table with multiple tables with same name...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;
                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_bigint;
                col2.isNullable = false;
                tbl.columns.Add(col2);

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Table)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != tbl.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrText != "Multiple Primary Key Columns")
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ParentName != layout.name)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_4  Ensure that the Verify_Database_Layout method call returns error if the given layout has a bad database name.
        [TestMethod]
        public async Task Test_1_2_4()
        {
            try
            {
                // Create layout with a bad database name...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890") + "_ *";
                layout.owner = "";

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Database)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != layout.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_5  Ensure that the Verify_Database_Layout method call returns error if the given layout has a bad table name.
        [TestMethod]
        public async Task Test_1_2_5()
        {
            try
            {
                // Create layout with a table with a bad table name...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890") + " _*";
                tbl.ordinal = 1;

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Table)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != tbl.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_6  Ensure that the Verify_Database_Layout method call returns error if the given layout has a bad column name.
        [TestMethod]
        public async Task Test_1_2_6()
        {
            try
            {
                // Create layout with a table with a bad column name...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;
                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890") + " _*";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                col1.isNullable = false;
                tbl.columns.Add(col1);

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != col1.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_2_7  Ensure that the Verify_Database_Layout method call returns error if the given layout has multiple columns in a table, with same name.
        [TestMethod]
        public async Task Test_1_2_7()
        {
            try
            {
                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout with a table with multiple columns, in same table, with same name...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;
                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col1.isNullable = true;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = col1.name;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col2.isNullable = true;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Database)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != layout.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }


        //  Test_1_3_1  Verify that the Verify_Database_Layout method call returns error if the layout's database doesn't exist.
        [TestMethod]
        public async Task Test_1_3_1()
        {
            try
            {
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create a simple layout...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;

                var res2 = dlt.Verify_Database_Layout(layout);
                if(res2.res != 0)
                    Assert.Fail("Wrong Value");

                if(res2.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res2.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Database)
                    Assert.Fail("Wrong Value");
                if (res2.errs[0].ObjName != dbname)
                    Assert.Fail("Wrong Value");
                if (res2.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
            }
        }

        //  Test_1_3_2  Verify that the Verify_Database_Layout method call returns error for a layout database owner that doesn't exist.
        [TestMethod]
        public async Task Test_1_3_2()
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

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create the live database with our admin user as the owner...
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
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
                }

                // Create layout with a database owner that doesn't exist...
                string dbowner = "testowner" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = dbowner;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Database)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != dbname)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Different)
                    Assert.Fail("Wrong Value");


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

        //  Test_1_3_3  Verify that the Verify_Database_Layout method call returns error for a layout database owner that is incorrect.
        [TestMethod]
        public async Task Test_1_3_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a unique owner...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
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
                    string mortaluser1_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                    var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");

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
                }


                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout with a table with a totally different user...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Database)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != dbname)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Different)
                    Assert.Fail("Wrong Value");


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

        //  Test_1_3_4  Verify that the Verify_Database_Layout method call returns error for a layout database table that doesn't exist.
        [TestMethod]
        public async Task Test_1_3_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout with a table that isn't in the live database...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                tbl.ordinal = 1;

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Table)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != tbl.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
                    Assert.Fail("Wrong Value");


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


        //  Test_1_4_0  Verify that the Verify_Database_Layout method call returns success for a layout that matches a live table with columns.
        [TestMethod]
        public async Task Test_1_4_0()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_Numeric_Column(colname, Postgres.DAL.Model.eNumericColTypes.integer, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col2.isNullable = true;
                tbl.columns.Add(col2);
                //DbLayout_Column col3 = new DbLayout_Column();
                //col3.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                //col3.ordinal = 3;
                //col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                //tbl.columns.Add(col3);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 1)
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

        //  Test_1_4_1  Verify that the Verify_Database_Layout method call returns error for a layout table column that doesn't exist in the live table.
        [TestMethod]
        public async Task Test_1_4_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_Numeric_Column(colname, Postgres.DAL.Model.eNumericColTypes.integer, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col2.isNullable = true;
                tbl.columns.Add(col2);
                DbLayout_Column col3 = new DbLayout_Column();
                col3.name = "testcol" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                col3.ordinal = 3;
                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col2.isNullable = true;
                tbl.columns.Add(col3);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != col3.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
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

        //  Test_1_4_2  Verify that the Verify_Database_Layout method call returns error for a live table column that doesn't exist in the layout table.
        [TestMethod]
        public async Task Test_1_4_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname3 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_Numeric_Column(colname2, Postgres.DAL.Model.eNumericColTypes.integer, true);
                    tch.Add_Numeric_Column(colname3, Postgres.DAL.Model.eNumericColTypes.integer, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col2.isNullable = true;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != colname3)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Extra)
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

        //  Test_1_4_3  Verify that the Verify_Database_Layout method call returns error for a table column whose type is different in the live table.
        [TestMethod]
        public async Task Test_1_4_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column(colname2, 47, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                col2.isNullable = true;
                col2.maxlength = 48;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != colname2)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Different)
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

        //  Test_1_4_3b  Verify that the Verify_Database_Layout method call returns success for a table column whose type is varchar.
        [TestMethod]
        public async Task Test_1_4_3b()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column(colname2, 47, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                col2.isNullable = true;
                col2.maxlength = 47;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 1)
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

        //  Test_1_4_4  Verify that the Verify_Database_Layout method call returns error for a varchar table column with a different max length in the live table.
        [TestMethod]
        public async Task Test_1_4_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column(colname2, 47, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                col2.isNullable = true;
                col2.maxlength = 48;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != colname2)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Different)
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

        //  Test_1_4_5  Verify that the Verify_Database_Layout method call returns error for a live table column with a different isnullable state than the layout.
        [TestMethod]
        public async Task Test_1_4_5()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column(colname2, 47, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                col2.isNullable = false;
                col2.maxlength = 47;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != colname2)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Different)
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

        //  Test_1_4_6  Verify that the Verify_Database_Layout method call returns error for a live table column that should be a primary key, but is not.
        [TestMethod]
        public async Task Test_1_4_6()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_Numeric_Column(colname2, Postgres.DAL.Model.eNumericColTypes.integer, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                col2.isNullable = false;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Table)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ParentName != dbname)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != tbl.name)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.ValidationError)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrText != "Multiple Primary Key Columns")
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

        //  Test_1_4_7  Verify that the Verify_Database_Layout method call returns error for a live table column that should NOT be a primary key, but is.
        [TestMethod]
        public async Task Test_1_4_7()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colname2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_Numeric_Column(colname2, Postgres.DAL.Model.eNumericColTypes.integer, true);

                    // Make the call to create the table...
                    var res6 = pt.Create_Table(tch);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl = new DbLayout_Table();
                layout.tables.Add(tbl);
                tbl.name = tblname;
                tbl.ordinal = 1;

                DbLayout_Column col1 = new DbLayout_Column();
                col1.name = "Id";
                col1.ordinal = 1;
                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col1.isNullable = false;
                tbl.columns.Add(col1);
                DbLayout_Column col2 = new DbLayout_Column();
                col2.name = colname2;
                col2.ordinal = 2;
                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.integer;
                col2.isNullable = true;
                tbl.columns.Add(col2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                if(res1.errs.Count != 1)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ObjName != "Id")
                    Assert.Fail("Wrong Value");
                if (res1.errs[0].ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.Different)
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


        //  Test_1_5_1  Verify that we can create a database with tables and columns, and a layout that verifies it.
        //              Then, verify that we can serialize the layout, deserialize it, and successfully verify the layout.
        [TestMethod]
        public async Task Test_1_5_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test table in our test database...
                string tblname1 = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname2 = "testtbl" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colnamet1c1 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colnamet1c2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colnamet1c3 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colnamet2c1 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colnamet2c2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    var tch1 = new TableDefinition(tblname1, pt.Username);
                    tch1.Add_Pk_Column(colnamet1c1, Postgres.DAL.Model.ePkColTypes.integer);
                    tch1.Add_Numeric_Column(colnamet1c2, Postgres.DAL.Model.eNumericColTypes.double_precision, true);
                    tch1.Add_Guid_Column(colnamet1c3, true);

                    // Make the call to create table1...
                    var res6 = pt.Create_Table(tch1);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(tblname1);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");

                    // Create the table definition...
                    var tch2 = new TableDefinition(tblname2, pt.Username);
                    tch2.Add_Pk_Column(colnamet2c1, Postgres.DAL.Model.ePkColTypes.integer);
                    tch2.Add_String_Column(colnamet2c2, 0, true);

                    // Make the call to create table2...
                    var res6a = pt.Create_Table(tch2);
                    if(res6a != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7b = pt.DoesTableExist(tblname2);
                    if(res7b != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout that matches what we built, above, but add an additional column...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl1 = new DbLayout_Table();
                layout.tables.Add(tbl1);
                tbl1.name = tblname1;
                tbl1.ordinal = 1;

                DbLayout_Column colt1c1 = new DbLayout_Column();
                colt1c1.name = colnamet1c1;
                colt1c1.ordinal = 1;
                colt1c1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                colt1c1.isNullable = false;
                tbl1.columns.Add(colt1c1);

                DbLayout_Column colt1c2 = new DbLayout_Column();
                colt1c2.name = colnamet1c2;
                colt1c2.ordinal = 2;
                colt1c2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.double_precision;
                colt1c2.isNullable = true;
                tbl1.columns.Add(colt1c2);

                DbLayout_Column colt1c3 = new DbLayout_Column();
                colt1c3.name = colnamet1c3;
                colt1c3.ordinal = 3;
                colt1c3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.uuid;
                colt1c3.isNullable = true;
                tbl1.columns.Add(colt1c3);

                DbLayout_Table tbl2 = new DbLayout_Table();
                layout.tables.Add(tbl2);
                tbl2.name = tblname2;
                tbl2.ordinal = 2;

                DbLayout_Column colt2c1 = new DbLayout_Column();
                colt2c1.name = colnamet2c1;
                colt2c1.ordinal = 1;
                colt2c1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                colt2c1.isNullable = false;
                tbl2.columns.Add(colt2c1);

                DbLayout_Column colt2c2 = new DbLayout_Column();
                colt2c2.name = colnamet2c2;
                colt2c2.ordinal = 2;
                colt2c2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                colt2c2.isNullable = true;
                tbl2.columns.Add(colt2c2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 1)
                    Assert.Fail("Wrong Value");


                // Serialize the layout...
                var jsonstring = Newtonsoft.Json.JsonConvert.SerializeObject(layout);


                // Now, deserialize the layout...
                var layout2 = Newtonsoft.Json.JsonConvert.DeserializeObject<DbLayout_Database>(jsonstring);


                // Use the deserialized layout to verify the database layout...
                var resver2 = dlt.Verify_Database_Layout(layout2);
                if(resver2.res != 1)
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

        //  Test_1_5_2  Verify that we can create a database with nothing in it, and a layout with tables and columns, and the verification call, reports all the differences.
        [TestMethod]
        public async Task Test_1_5_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string testtable1 = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string testtable2 = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }


                // Create one of the test tables in our test database...
                string colnamet2c1 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string colnamet2c2 = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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


                    // Create a table definition without any columns...
                    var tch1 = new TableDefinition(testtable1, pt.Username);

                    // Make the call to create table1...
                    var res6a = pt.Create_Table(tch1);
                    if(res6a != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7a = pt.DoesTableExist(testtable1);
                    if(res7a != 1)
                        Assert.Fail("Wrong Value");


                    // Create the table definition, but leave out one column...
                    var tch2 = new TableDefinition(testtable2, pt.Username);
                    tch2.Add_Pk_Column(colnamet2c1, Postgres.DAL.Model.ePkColTypes.integer);
                    //tch2.Add_String_Column(colnamet2c2, 0, true);

                    // Make the call to create table2...
                    var res6 = pt.Create_Table(tch2);
                    if(res6 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res7 = pt.DoesTableExist(testtable2);
                    if(res7 != 1)
                        Assert.Fail("Wrong Value");
                }


                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create layout with some tables and columns...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";
                DbLayout_Table tbl1 = new DbLayout_Table();
                layout.tables.Add(tbl1);
                tbl1.name = testtable1;
                tbl1.ordinal = 1;

                DbLayout_Column colt1c1 = new DbLayout_Column();
                colt1c1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                colt1c1.ordinal = 1;
                colt1c1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                colt1c1.isNullable = false;
                tbl1.columns.Add(colt1c1);

                DbLayout_Column colt1c2 = new DbLayout_Column();
                colt1c2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                colt1c2.ordinal = 2;
                colt1c2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.double_precision;
                colt1c2.isNullable = true;
                tbl1.columns.Add(colt1c2);

                DbLayout_Column colt1c3 = new DbLayout_Column();
                colt1c3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                colt1c3.ordinal = 3;
                colt1c3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.uuid;
                colt1c3.isNullable = true;
                tbl1.columns.Add(colt1c3);

                DbLayout_Table tbl2 = new DbLayout_Table();
                layout.tables.Add(tbl2);
                tbl2.name = testtable2;
                tbl2.ordinal = 2;

                DbLayout_Column colt2c1 = new DbLayout_Column();
                colt2c1.name = colnamet2c1;
                colt2c1.ordinal = 1;
                colt2c1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_integer;
                colt2c1.isNullable = false;
                tbl2.columns.Add(colt2c1);

                DbLayout_Column colt2c2 = new DbLayout_Column();
                colt2c2.name = colnamet2c2;
                colt2c2.ordinal = 2;
                colt2c2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                colt2c2.isNullable = true;
                tbl2.columns.Add(colt2c2);

                var res1 = dlt.Verify_Database_Layout(layout);
                if(res1.res != 0)
                    Assert.Fail("Wrong Value");

                // Should be three missing columns from one table, and one missing column from another...
                if(res1.errs.Count != 4)
                    Assert.Fail("Wrong Value");

                var mt1c1 = res1.errs.FirstOrDefault(n => n.ParentName == tbl1.name && n.ObjName == colt1c1.name);
                if(mt1c1 == null)
                    Assert.Fail("Failed to report missing column");
                if(mt1c1.ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if(mt1c1.ParentName != tbl1.name)
                    Assert.Fail("Wrong Value");
                if(mt1c1.ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
                    Assert.Fail("Wrong Value");

                var mt1c2 = res1.errs.FirstOrDefault(n => n.ParentName == tbl1.name && n.ObjName == colt1c2.name);
                if(mt1c2 == null)
                    Assert.Fail("Failed to report missing column");
                if(mt1c2.ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if(mt1c2.ParentName != tbl1.name)
                    Assert.Fail("Wrong Value");
                if(mt1c2.ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
                    Assert.Fail("Wrong Value");

                var mt1c3 = res1.errs.FirstOrDefault(n => n.ParentName == tbl1.name && n.ObjName == colt1c3.name);
                if(mt1c3 == null)
                    Assert.Fail("Failed to report missing column");
                if(mt1c3.ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if(mt1c3.ParentName != tbl1.name)
                    Assert.Fail("Wrong Value");
                if(mt1c3.ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
                    Assert.Fail("Wrong Value");

                var mt2c2 = res1.errs.FirstOrDefault(n => n.ParentName == tbl2.name && n.ObjName == colt2c2.name);
                if(mt2c2 == null)
                    Assert.Fail("Failed to report missing column");
                if(mt2c2.ObjType != Postgres.DAL.CreateVerify.Model.eObjType.Column)
                    Assert.Fail("Wrong Value");
                if(mt2c2.ParentName != tbl2.name)
                    Assert.Fail("Wrong Value");
                if(mt2c2.ErrorType != Postgres.DAL.CreateVerify.Model.eErrorType.NotFound)
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


        //  Test_1_6_1  Verify that an error results if we attempt to create create a live database from a database layout, but the database already exists.
        [TestMethod]
        public async Task Test_1_6_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Check that the database doesn't exist...
                    var res1a = pt.Is_Database_Present(dbname);
                    if(res1a != 0)
                        Assert.Fail("Wrong Value");

                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Is_Database_Present(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Create a layout using the same database name...
                DbLayout_Database layout = new DbLayout_Database();
                layout.name = dbname;
                layout.owner = "";

                var res1 = dlt.Create_Database_fromLayout(layout);
                if(res1.res != -3)
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

        //  Test_1_6_2  Verify that we can create a live database from a database layout.
        [TestMethod]
        public async Task Test_1_6_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Create a database layout of moderate complexity...
                DbLayout_Database layout = new DbLayout_Database();
                {
                    layout.name = dbname;
                    layout.owner = "postgres";

                    // Add table 1...
                    {
                        DbLayout_Table tbl1 = new DbLayout_Table();
                        tbl1.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                        tbl1.ordinal = 1;

                        // Add columns...
                        {
                            var col1 = new DbLayout_Column();
                            col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col1.ordinal = 1;
                            col1.maxlength = null;
                            col1.isNullable = false;
                            col1.isIdentity = false;
                            col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                            tbl1.columns.Add(col1);

                            var col2 = new DbLayout_Column();
                            col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col2.ordinal = 2;
                            col2.maxlength = null;
                            col2.isNullable = true;
                            col2.isIdentity = false;
                            col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                            tbl1.columns.Add(col2);

                            var col3 = new DbLayout_Column();
                            col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col3.ordinal = 3;
                            col3.maxlength = 50;
                            col3.isNullable = false;
                            col3.isIdentity = false;
                            col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                            tbl1.columns.Add(col3);

                            var col4 = new DbLayout_Column();
                            col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col4.ordinal = 4;
                            col4.maxlength = null;
                            col4.isNullable = true;
                            col4.isIdentity = false;
                            col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                            tbl1.columns.Add(col4);

                            var col5 = new DbLayout_Column();
                            col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col5.ordinal = 5;
                            col5.maxlength = null;
                            col5.isNullable = true;
                            col5.isIdentity = false;
                            col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                            tbl1.columns.Add(col5);
                        }

                        layout.tables.Add(tbl1);
                    }

                    // Add table 2...
                    {
                        DbLayout_Table tbl2 = new DbLayout_Table();
                        tbl2.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                        tbl2.ordinal = 2;

                        // Add columns...
                        {
                            var col1 = new DbLayout_Column();
                            col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col1.ordinal = 1;
                            col1.maxlength = null;
                            col1.isNullable = false;
                            col1.isIdentity = false;
                            col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                            tbl2.columns.Add(col1);

                            var col2 = new DbLayout_Column();
                            col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col2.ordinal = 2;
                            col2.maxlength = null;
                            col2.isNullable = true;
                            col2.isIdentity = false;
                            col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                            tbl2.columns.Add(col2);

                            var col3 = new DbLayout_Column();
                            col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col3.ordinal = 3;
                            col3.maxlength = 50;
                            col3.isNullable = false;
                            col3.isIdentity = false;
                            col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                            tbl2.columns.Add(col3);

                            var col4 = new DbLayout_Column();
                            col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col4.ordinal = 4;
                            col4.maxlength = null;
                            col4.isNullable = true;
                            col4.isIdentity = false;
                            col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                            tbl2.columns.Add(col4);

                            var col5 = new DbLayout_Column();
                            col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            col5.ordinal = 5;
                            col5.maxlength = null;
                            col5.isNullable = true;
                            col5.isIdentity = false;
                            col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                            tbl2.columns.Add(col5);
                        }

                        layout.tables.Add(tbl2);
                    }
                }

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;

                // Attempt to create the live database from the generated layout...
                var res1 = dlt.Create_Database_fromLayout(layout);
                if(res1.res != 1)
                    Assert.Fail("Wrong Value");


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

        //  Test_1_6_3  Verify that we can verify a database layout against a live database.
        [TestMethod]
        public async Task Test_1_6_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;


                // Create a live database, via layout...
                DbLayout_Database layout = new DbLayout_Database();
                {
                    // Create a database layout of moderate complexity...
                    {
                        layout.name = dbname;
                        layout.owner = "postgres";

                        // Add table 1...
                        {
                            DbLayout_Table tbl1 = new DbLayout_Table();
                            tbl1.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            tbl1.ordinal = 1;

                            // Add columns...
                            {
                                var col1 = new DbLayout_Column();
                                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col1.ordinal = 1;
                                col1.maxlength = null;
                                col1.isNullable = false;
                                col1.isIdentity = false;
                                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                                tbl1.columns.Add(col1);

                                var col2 = new DbLayout_Column();
                                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col2.ordinal = 2;
                                col2.maxlength = null;
                                col2.isNullable = true;
                                col2.isIdentity = false;
                                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                                tbl1.columns.Add(col2);

                                var col3 = new DbLayout_Column();
                                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col3.ordinal = 3;
                                col3.maxlength = 50;
                                col3.isNullable = false;
                                col3.isIdentity = false;
                                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                                tbl1.columns.Add(col3);

                                var col4 = new DbLayout_Column();
                                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col4.ordinal = 4;
                                col4.maxlength = null;
                                col4.isNullable = true;
                                col4.isIdentity = false;
                                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                                tbl1.columns.Add(col4);

                                var col5 = new DbLayout_Column();
                                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col5.ordinal = 5;
                                col5.maxlength = null;
                                col5.isNullable = true;
                                col5.isIdentity = false;
                                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                                tbl1.columns.Add(col5);
                            }

                            layout.tables.Add(tbl1);
                        }

                        // Add table 2...
                        {
                            DbLayout_Table tbl2 = new DbLayout_Table();
                            tbl2.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            tbl2.ordinal = 2;

                            // Add columns...
                            {
                                var col1 = new DbLayout_Column();
                                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col1.ordinal = 1;
                                col1.maxlength = null;
                                col1.isNullable = false;
                                col1.isIdentity = false;
                                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                                tbl2.columns.Add(col1);

                                var col2 = new DbLayout_Column();
                                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col2.ordinal = 2;
                                col2.maxlength = null;
                                col2.isNullable = true;
                                col2.isIdentity = false;
                                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                                tbl2.columns.Add(col2);

                                var col3 = new DbLayout_Column();
                                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col3.ordinal = 3;
                                col3.maxlength = 50;
                                col3.isNullable = false;
                                col3.isIdentity = false;
                                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                                tbl2.columns.Add(col3);

                                var col4 = new DbLayout_Column();
                                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col4.ordinal = 4;
                                col4.maxlength = null;
                                col4.isNullable = true;
                                col4.isIdentity = false;
                                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                                tbl2.columns.Add(col4);

                                var col5 = new DbLayout_Column();
                                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col5.ordinal = 5;
                                col5.maxlength = null;
                                col5.isNullable = true;
                                col5.isIdentity = false;
                                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                                tbl2.columns.Add(col5);
                            }

                            layout.tables.Add(tbl2);
                        }
                    }

                    // Attempt to create the live database from the generated layout...
                    var res1 = dlt.Create_Database_fromLayout(layout);
                    if(res1.res != 1)
                        Assert.Fail("Wrong Value");
                }


                // Verify the live database matches our layout...
                var res2 = dlt.Verify_Database_Layout(layout);
                if(res2.res != 1 || res2.errs == null)
                    Assert.Fail("Wrong Value");

                // Check for errors...
                if(res2.errs.Count != 0)
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

        //  Test_1_6_4  Verify that we can create a database layout from a live database.
        [TestMethod]
        public async Task Test_1_6_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;


                // Create a live database, via layout...
                DbLayout_Database layout = new DbLayout_Database();
                {
                    // Create a database layout of moderate complexity...
                    {
                        layout.name = dbname;
                        layout.owner = "postgres";

                        // Add table 1...
                        {
                            DbLayout_Table tbl1 = new DbLayout_Table();
                            tbl1.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            tbl1.ordinal = 1;

                            // Add columns...
                            {
                                var col1 = new DbLayout_Column();
                                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col1.ordinal = 1;
                                col1.maxlength = null;
                                col1.isNullable = false;
                                col1.isIdentity = false;
                                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                                tbl1.columns.Add(col1);

                                var col2 = new DbLayout_Column();
                                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col2.ordinal = 2;
                                col2.maxlength = null;
                                col2.isNullable = true;
                                col2.isIdentity = false;
                                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                                tbl1.columns.Add(col2);

                                var col3 = new DbLayout_Column();
                                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col3.ordinal = 3;
                                col3.maxlength = 50;
                                col3.isNullable = false;
                                col3.isIdentity = false;
                                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                                tbl1.columns.Add(col3);

                                var col4 = new DbLayout_Column();
                                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col4.ordinal = 4;
                                col4.maxlength = null;
                                col4.isNullable = true;
                                col4.isIdentity = false;
                                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                                tbl1.columns.Add(col4);

                                var col5 = new DbLayout_Column();
                                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col5.ordinal = 5;
                                col5.maxlength = null;
                                col5.isNullable = true;
                                col5.isIdentity = false;
                                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                                tbl1.columns.Add(col5);
                            }

                            layout.tables.Add(tbl1);
                        }

                        // Add table 2...
                        {
                            DbLayout_Table tbl2 = new DbLayout_Table();
                            tbl2.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            tbl2.ordinal = 2;

                            // Add columns...
                            {
                                var col1 = new DbLayout_Column();
                                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col1.ordinal = 1;
                                col1.maxlength = null;
                                col1.isNullable = false;
                                col1.isIdentity = false;
                                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                                tbl2.columns.Add(col1);

                                var col2 = new DbLayout_Column();
                                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col2.ordinal = 2;
                                col2.maxlength = null;
                                col2.isNullable = true;
                                col2.isIdentity = false;
                                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                                tbl2.columns.Add(col2);

                                var col3 = new DbLayout_Column();
                                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col3.ordinal = 3;
                                col3.maxlength = 50;
                                col3.isNullable = false;
                                col3.isIdentity = false;
                                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                                tbl2.columns.Add(col3);

                                var col4 = new DbLayout_Column();
                                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col4.ordinal = 4;
                                col4.maxlength = null;
                                col4.isNullable = true;
                                col4.isIdentity = false;
                                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                                tbl2.columns.Add(col4);

                                var col5 = new DbLayout_Column();
                                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col5.ordinal = 5;
                                col5.maxlength = null;
                                col5.isNullable = true;
                                col5.isIdentity = false;
                                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                                tbl2.columns.Add(col5);
                            }

                            layout.tables.Add(tbl2);
                        }
                    }

                    // Attempt to create the live database from the generated layout...
                    var res1 = dlt.Create_Database_fromLayout(layout);
                    if(res1.res != 1)
                        Assert.Fail("Wrong Value");
                }

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

        //  Test_1_6_5  Verify that we can use a generated database layout, to create an identical live database.
        [TestMethod]
        public async Task Test_1_6_5()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we will verify...
                string dbname = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                var dlt = new DatabaseLayout_Tool();
                dlt.Hostname = dbcreds.Host;
                dlt.Username = dbcreds.User;
                dlt.Password = dbcreds.Password;


                // Create a live database, via layout...
                DbLayout_Database layout = new DbLayout_Database();
                {
                    // Create a database layout of moderate complexity...
                    {
                        layout.name = dbname;
                        layout.owner = "postgres";

                        // Add table 1...
                        {
                            DbLayout_Table tbl1 = new DbLayout_Table();
                            tbl1.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            tbl1.ordinal = 1;

                            // Add columns...
                            {
                                var col1 = new DbLayout_Column();
                                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col1.ordinal = 1;
                                col1.maxlength = null;
                                col1.isNullable = false;
                                col1.isIdentity = false;
                                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                                tbl1.columns.Add(col1);

                                var col2 = new DbLayout_Column();
                                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col2.ordinal = 2;
                                col2.maxlength = null;
                                col2.isNullable = true;
                                col2.isIdentity = false;
                                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                                tbl1.columns.Add(col2);

                                var col3 = new DbLayout_Column();
                                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col3.ordinal = 3;
                                col3.maxlength = 50;
                                col3.isNullable = false;
                                col3.isIdentity = false;
                                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                                tbl1.columns.Add(col3);

                                var col4 = new DbLayout_Column();
                                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col4.ordinal = 4;
                                col4.maxlength = null;
                                col4.isNullable = true;
                                col4.isIdentity = false;
                                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                                tbl1.columns.Add(col4);

                                var col5 = new DbLayout_Column();
                                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col5.ordinal = 5;
                                col5.maxlength = null;
                                col5.isNullable = true;
                                col5.isIdentity = false;
                                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                                tbl1.columns.Add(col5);
                            }

                            layout.tables.Add(tbl1);
                        }

                        // Add table 2...
                        {
                            DbLayout_Table tbl2 = new DbLayout_Table();
                            tbl2.name = "testtable" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                            tbl2.ordinal = 2;

                            // Add columns...
                            {
                                var col1 = new DbLayout_Column();
                                col1.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col1.ordinal = 1;
                                col1.maxlength = null;
                                col1.isNullable = false;
                                col1.isIdentity = false;
                                col1.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.pk_uuid;
                                tbl2.columns.Add(col1);

                                var col2 = new DbLayout_Column();
                                col2.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col2.ordinal = 2;
                                col2.maxlength = null;
                                col2.isNullable = true;
                                col2.isIdentity = false;
                                col2.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.bigint;
                                tbl2.columns.Add(col2);

                                var col3 = new DbLayout_Column();
                                col3.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col3.ordinal = 3;
                                col3.maxlength = 50;
                                col3.isNullable = false;
                                col3.isIdentity = false;
                                col3.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.varchar;
                                tbl2.columns.Add(col3);

                                var col4 = new DbLayout_Column();
                                col4.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col4.ordinal = 4;
                                col4.maxlength = null;
                                col4.isNullable = true;
                                col4.isIdentity = false;
                                col4.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.timestampUTC;
                                tbl2.columns.Add(col4);

                                var col5 = new DbLayout_Column();
                                col5.name = "testcolumn" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                                col5.ordinal = 5;
                                col5.maxlength = null;
                                col5.isNullable = true;
                                col5.isIdentity = false;
                                col5.dataType = Postgres.DAL.CreateVerify.Model.eColDataTypes.text;
                                tbl2.columns.Add(col5);
                            }

                            layout.tables.Add(tbl2);
                        }
                    }

                    // Attempt to create the live database from the generated layout...
                    var res1 = dlt.Create_Database_fromLayout(layout);
                    if(res1.res != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create layout from live database...
                var res2 = dlt.CreateLayout_fromDatabase(dbname);
                if(res2.res != 1 || res2.layout == null)
                    Assert.Fail("Wrong Value");


                // Verify created layout is a match to what we generated the live database from...
                var res3 = DbLayout_Database.CompareLayouts(layout, res2.layout);
                if(res3 != 1)
                    Assert.Fail("Layout Mismatch");


                // Serialize the layout, to simulate saving it to disk...
                var jsonlayout = Newtonsoft.Json.JsonConvert.SerializeObject(res2.layout, Newtonsoft.Json.Formatting.Indented);


                // Deserialize the layout, to simulate loading it from disk...
                var layout2 = Newtonsoft.Json.JsonConvert.DeserializeObject<DbLayout_Database>(jsonlayout);

                // Give the "retrieved" database a different name, so we can create a database on the same test host...
                layout2.name = "testdb" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Use the "retrieved" layout to create an identical database...
                var res4 = dlt.Create_Database_fromLayout(layout2);
                if(res4.res != 1 || res4.errs == null || res4.errs.Count != 0)
                    Assert.Fail("Failed to create second database");


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

                // Delete the second database...
                var res10 = pt.Drop_Database(layout2.name, true);
                if(res10 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the second database is no longer present...
                var res11 = pt.Is_Database_Present(layout2.name);
                if(res11 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        #endregion
        

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

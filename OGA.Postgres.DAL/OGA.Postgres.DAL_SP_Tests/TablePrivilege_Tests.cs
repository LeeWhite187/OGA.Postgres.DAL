using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.Postgres;
using OGA.SharedKernel.Process;
using OGA.SharedKernel;
using OGA.Testing.Lib;
using System;
using System.Collections.Generic;
using System.Web;
using OGA.Common.Config.structs;
using OGA.Postgres.DAL;
using System.Threading.Tasks;

namespace OGA.Postgres_Tests
{
    /*  Unit Tests for PostgreSQL Tools class.
        This set of tests exercise the methods: SetTablePrivilegesforUser() and GetTablePrivilegesforUser().

        //  Test_1_1_1  Verify that we can query user privileges for a table.
        //  Test_1_1_2  Verify that a query of user privileges for a table fails if the table is not present.
        //  Test_1_1_3  Verify that a query of user privileges for a table fails if the user does not exist.
        //  Test_1_1_4  Verify that a query of user privileges for a table fails if the table is not in the connected database.
        //  Test_1_1_5  Verify that a query of user privileges for a table returns all privileges for a user granted ALL on a table.
        //  Test_1_1_6  Verify that a query of user privileges for a table returns no privileges for a user revoked ALL on a table.

        //  Test_1_2_01 Verify that we can add SELECT privilege on a table for a user.
        //  Test_1_2_02 Verify that we can remove SELECT privilege from a table for a user.
        //  Test_1_2_03 Verify that we can add INSERT privilege on a table for a user.
        //  Test_1_2_04 Verify that we can remove INSERT privilege from a table for a user.
        //  Test_1_2_05 Verify that we can add UPDATE privilege on a table for a user.
        //  Test_1_2_06 Verify that we can remove UPDATE privilege from a table for a user.
        //  Test_1_2_07 Verify that we can add DELETE privilege on a table for a user.
        //  Test_1_2_08 Verify that we can remove DELETE privilege from a table for a user.
        //  Test_1_2_09 Verify that we can add TRUNCATE privilege on a table for a user.
        //  Test_1_2_10 Verify that we can remove TRUNCATE privilege from a table for a user.
        //  Test_1_2_11 Verify that we can add REFERENCES privilege on a table for a user.
        //  Test_1_2_12 Verify that we can remove REFERENCES privilege from a table for a user.
        //  Test_1_2_13 Verify that we can add TRIGGER privilege on a table for a user.
        //  Test_1_2_14 Verify that we can remove TRIGGER privilege from a table for a user.

    */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class TablePrivilege_Tests : OGA.Testing.Lib.Test_Base_abstract
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


        //  Test_1_1_1  Verify that we can query user privileges for a table.
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add SELECT privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has SELECT privileges on the test table...
                if(privs2 != eTablePrivileges.SELECT)
                    Assert.Fail("Wrong Value");

                // To drop the database, we must switch back to the postgres database...
                {
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

        //  Test_1_1_2  Verify that a query of user privileges for a table fails if the table is not present.
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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

                    // Skip creating the table...
                    //// Create the table definition...
                    //var tch = new TableDefinition(tblname, pt.Username);
                    //tch.Add_Pk_Column("Id", Postgres.DAL.Model.ePkColTypes.integer);
                    //tch.Add_String_Column("IconName", 50, false);

                    //// Make the call to create the table...
                    //var res3 = pt.Create_Table(tch);
                    //if(res3 != 1)
                    //    Assert.Fail("Wrong Value");

                    //// Confirm the table was created...
                    //var res3a = pt.DoesTableExist(tblname);
                    //if(res3a != 1)
                    //    Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != -1)
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

        //  Test_1_1_3  Verify that a query of user privileges for a table fails if the user does not exist.
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

                // Create a test database with a table we can test with...
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }


                // Check table privileges for a bogus user...
                string bogususer = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res4 = pt.GetTablePrivilegesforUser(tblname, bogususer , out var privs);
                if(res4 != -1)
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

        //  Test_1_1_4  Verify that a query of user privileges for a table fails if the table is not in the connected database.
        [TestMethod]
        public async Task Test_1_1_4()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");


                // Swap connection back to the system catalog...
                {
                    // Swap our connection back to the postgres database...
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = new Postgres_Tools();
                    pt.Hostname = dbcreds.Host;
                    pt.Database = dbcreds.Database;
                    pt.Username = dbcreds.User;
                    pt.Password = dbcreds.Password;

                    // Verify we can access the system catalog...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Attempt to check table privileges for the test user on the table, while not connected to its parent database...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != -1)
                    Assert.Fail("Wrong Value");


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

        //  Test_1_1_5  Verify that a query of user privileges for a table returns all privileges for a user granted ALL on a table.
        [TestMethod]
        public async Task Test_1_1_5()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add all privileges for the test user...
                eTablePrivileges privstoset = eTablePrivileges.DELETE |
                                              eTablePrivileges.INSERT |
                                              eTablePrivileges.REFERENCES |
                                              eTablePrivileges.SELECT |
                                              eTablePrivileges.TRIGGER |
                                              eTablePrivileges.TRUNCATE |
                                              eTablePrivileges.UPDATE;
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, privstoset, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has all privileges on the test table...
                if(privs2 != (eTablePrivileges.DELETE |
                              eTablePrivileges.INSERT |
                              eTablePrivileges.REFERENCES |
                              eTablePrivileges.SELECT |
                              eTablePrivileges.TRIGGER |
                              eTablePrivileges.TRUNCATE |
                              eTablePrivileges.UPDATE))
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

        //  Test_1_1_6  Verify that a query of user privileges for a table returns no privileges for a user revoked ALL on a table.
        [TestMethod]
        public async Task Test_1_1_6()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add SELECT privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has SELECT privileges on the test table...
                if(privs2 != eTablePrivileges.SELECT)
                    Assert.Fail("Wrong Value");


                // Revoke all privileges of the test user on the test table...
                var res6a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.NONE, tblname);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6b = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6b != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has no privileges on the test table...
                if(privs3 != eTablePrivileges.NONE)
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


        //  Test_1_2_01 Verify that we can add SELECT privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_01()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add SELECT privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has SELECT privileges on the test table...
                if(privs2 != eTablePrivileges.SELECT)
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

        //  Test_1_2_02 Verify that we can remove SELECT privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_02()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including SELECT privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the SELECT privilege on the test table...
                if((privs2 & eTablePrivileges.SELECT) != eTablePrivileges.SELECT)
                    Assert.Fail("Wrong Value");


                // Remove the SELECT privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the SELECT privilege on the test table...
                if(privs3 != (eTablePrivileges.INSERT | eTablePrivileges.DELETE))
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

        //  Test_1_2_03 Verify that we can add INSERT privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_03()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add INSERT privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has INSERT privileges on the test table...
                if(privs2 != eTablePrivileges.INSERT)
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

        //  Test_1_2_04 Verify that we can remove INSERT privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_04()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including INSERT privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the INSERT privilege on the test table...
                if((privs2 & eTablePrivileges.INSERT) != eTablePrivileges.INSERT)
                    Assert.Fail("Wrong Value");


                // Remove the INSERT privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT | eTablePrivileges.DELETE, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the INSERT privilege on the test table...
                if(privs3 != (eTablePrivileges.SELECT | eTablePrivileges.DELETE))
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

        //  Test_1_2_05 Verify that we can add UPDATE privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_05()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add UPDATE privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.UPDATE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has UPDATE privileges on the test table...
                if(privs2 != eTablePrivileges.UPDATE)
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

        //  Test_1_2_06 Verify that we can remove UPDATE privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_06()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including UPDATE privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.UPDATE | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the UPDATE privilege on the test table...
                if((privs2 & eTablePrivileges.UPDATE) != eTablePrivileges.UPDATE)
                    Assert.Fail("Wrong Value");


                // Remove the UPDATE privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the UPDATE privilege on the test table...
                if(privs3 != (eTablePrivileges.INSERT | eTablePrivileges.DELETE))
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

        //  Test_1_2_07 Verify that we can add DELETE privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_07()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add DELETE privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has DELETE privileges on the test table...
                if(privs2 != eTablePrivileges.DELETE)
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

        //  Test_1_2_08 Verify that we can remove DELETE privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_08()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including DELETE privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.SELECT | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the DELETE privilege on the test table...
                if((privs2 & eTablePrivileges.DELETE) != eTablePrivileges.DELETE)
                    Assert.Fail("Wrong Value");


                // Remove the DELETE privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT | eTablePrivileges.SELECT, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the DELETE privilege on the test table...
                if(privs3 != (eTablePrivileges.INSERT | eTablePrivileges.SELECT))
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

        //  Test_1_2_09 Verify that we can add TRUNCATE privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_09()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add TRUNCATE privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.TRUNCATE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has TRUNCATE privileges on the test table...
                if(privs2 != eTablePrivileges.TRUNCATE)
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

        //  Test_1_2_10 Verify that we can remove TRUNCATE privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_10()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including TRUNCATE privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.TRUNCATE | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the TRUNCATE privilege on the test table...
                if((privs2 & eTablePrivileges.TRUNCATE) != eTablePrivileges.TRUNCATE)
                    Assert.Fail("Wrong Value");


                // Remove the TRUNCATE privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the TRUNCATE privilege on the test table...
                if(privs3 != (eTablePrivileges.INSERT | eTablePrivileges.DELETE))
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

        //  Test_1_2_11 Verify that we can add REFERENCES privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_11()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add REFERENCES privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.REFERENCES, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has REFERENCES privileges on the test table...
                if(privs2 != eTablePrivileges.REFERENCES)
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

        //  Test_1_2_12 Verify that we can remove REFERENCES privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_12()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including REFERENCES privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.REFERENCES | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the REFERENCES privilege on the test table...
                if((privs2 & eTablePrivileges.REFERENCES) != eTablePrivileges.REFERENCES)
                    Assert.Fail("Wrong Value");


                // Remove the REFERENCES privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the REFERENCES privilege on the test table...
                if(privs3 != (eTablePrivileges.INSERT | eTablePrivileges.DELETE))
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

        //  Test_1_2_13 Verify that we can add TRIGGER privilege on a table for a user.
        [TestMethod]
        public async Task Test_1_2_13()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add TRIGGER privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.TRIGGER, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user now has TRIGGER privileges on the test table...
                if(privs2 != eTablePrivileges.TRIGGER)
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

        //  Test_1_2_14 Verify that we can remove TRIGGER privilege from a table for a user.
        [TestMethod]
        public async Task Test_1_2_14()
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
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
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
                    tch.Add_String_Column("IconName", 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }

                // Create a test user...
                string mortaluser1 = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check table privileges for the test user...
                var res4 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user has no privileges on the test table...
                if(privs != eTablePrivileges.NONE)
                    Assert.Fail("Wrong Value");


                // Add a few privileges, including TRIGGER privilege for the test user...
                var res5 = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.TRIGGER | eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");


                // Check updated privileges for the test user...
                var res6 = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs2);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges includes the TRIGGER privilege on the test table...
                if((privs2 & eTablePrivileges.TRIGGER) != eTablePrivileges.TRIGGER)
                    Assert.Fail("Wrong Value");


                // Remove the TRIGGER privilege for the test user...
                var res5a = pt.SetTablePrivilegesforUser(mortaluser1, eTablePrivileges.INSERT | eTablePrivileges.DELETE, tblname);
                if(res5a != 1)
                    Assert.Fail("Wrong Value");

                // Check updated privileges for the test user...
                var res6a = pt.GetTablePrivilegesforUser(tblname, mortaluser1, out var privs3);
                if(res6a != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user's privileges no longer include the TRIGGER privilege on the test table...
                if(privs3 != (eTablePrivileges.INSERT | eTablePrivileges.DELETE))
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


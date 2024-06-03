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

namespace OGA.Postgres_Tests
{
    /*  Unit Tests for PostgreSQL Tools class.
        This set of tests exercise the user management methods.

        //  Test_1_1_1  Verify that we can query for a list of users.

        //  Test_1_2_1  Verify username validator limits to letters, numbers, and underscores.
     
        //  Test_1_3_1  Verify that we can add and delete a user.
        //  Test_1_3_2  Verify that no error results from deleting a nonexistant user.
        //  Test_1_3_3  Verify that a nonsuperuser cannot add and delete a user.

        //  Test_1_4_1  Verify that we can change a test user's password.
        //  Test_1_4_2  Verify that a non superuser cannot change a test user's password.
        //  Test_1_4_3  Verify that a non superuser can change their own password.

        //  Test_1_5_1  Verify that we can check a superuser is actually a superuser.
        //  Test_1_5_2  Verify that we can check a non superuser is not a superuser.
        //  Test_1_5_3  Verify that we can promote a user to superuser.
        //  Test_1_5_4  Verify that we can demote a user from superuser to regular user.

        //  Test_1_6_1  Verify that we can check a user has CreateDB.
        //  Test_1_6_2  Verify that we can check a user does not have CreateDB.
        //  Test_1_6_3  Verify that we can grant CreateDB to a user.
        //  Test_1_6_4  Verify that we can deny CreateDB to a user.
     
        //  Test_1_7_1  Verify that we can check a user has CreateRole.
        //  Test_1_7_2  Verify that we can check a user does not have CreateRole.
        //  Test_1_7_3  Verify that we can grant CreateRole to a user.
        //  Test_1_7_4  Verify that we can deny CreateRole to a user.

     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class UserMgmt_Tests : OGA.Testing.Lib.Test_Base_abstract
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

        
        //  Test_1_1_1  Verify that we can query for a list of users.
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

                // Create a test user...
                string mortaluser1 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Get a list of system users...
                var res4 = pt.GetUserList(out var userlist);
                if(res4 != 1 || userlist == null || userlist.Count == 0)
                    Assert.Fail("Wrong Value");

                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");

                // Delete the test user...
                var res7 = pt.DeleteUser(mortaluser1);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the user is no longer present...
                var res8 = pt.Does_Login_Exist(mortaluser1);
                if(res8 != 0)
                    Assert.Fail("Wrong Value");

                // Get a list of system users...
                var res4a = pt.GetUserList(out var userlist2);
                if(res4a != 1 || userlist2 == null || userlist2.Count == 0)
                    Assert.Fail("Wrong Value");

                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_2_1  Verify username validator limits to letters, numbers, and underscores.
        [TestMethod]
        public void Test_1_2_1()
        {
            var res1 = Postgres_Tools.UserNameIsValid("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_");
            if (!res1)
                Assert.Fail("Wrong Value");

            var res2 = Postgres_Tools.UserNameIsValid("");
            if (res2)
                Assert.Fail("Wrong Value");

            var res3 = Postgres_Tools.UserNameIsValid(" ");
            if (res3)
                Assert.Fail("Wrong Value");

            var res4 = Postgres_Tools.UserNameIsValid("a ");
            if (res4)
                Assert.Fail("Wrong Value");

            var res5 = Postgres_Tools.UserNameIsValid(" a");
            if (res5)
                Assert.Fail("Wrong Value");

            var res6 = Postgres_Tools.UserNameIsValid("sadfsdf.assdds");
            if (res6)
                Assert.Fail("Wrong Value");

            var res7 = Postgres_Tools.UserNameIsValid("sadfsdf+assdds");
            if (res7)
                Assert.Fail("Wrong Value");
        }


        //  Test_1_3_1  Verify that we can add and delete a user.
        [TestMethod]
        public void Test_1_3_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Username = dbcreds.User;
                pt.Hostname = dbcreds.Host;
                pt.Password = dbcreds.Password;
                pt.Database = dbcreds.Database;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res2 = pt.DeleteUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_3_2  Verify that no error results from deleting a nonexistant user.
        [TestMethod]
        public void Test_1_3_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Username = dbcreds.User;
                pt.Hostname = dbcreds.Host;
                pt.Password = dbcreds.Password;
                pt.Database = dbcreds.Database;

                // Create a test username...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");

                // Verify it doesn't exist...
                var res2 = pt.Does_Login_Exist(username);
                if(res2 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete the user...
                var res3 = pt.DeleteUser(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_3_3  Verify that a nonsuperuser cannot add and delete a user.
        [TestMethod]
        public void Test_1_3_3()
        {
            Postgres_Tools pt = null;
            Postgres_Tools pt2 = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a non superuser...
                string mortaluser = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res1 = pt.CreateUser(mortaluser, mortaluser_password);
                if(res1 != 1)
                    Assert.Fail("Wrong Value");

                // Open a connection with the non-superuser...
                pt2 = new Postgres_Tools();
                pt2.Hostname = dbcreds.Host;
                pt2.Database = dbcreds.Database;
                pt2.Username = mortaluser;
                pt2.Password = mortaluser_password;

                // Have the mortal user attempt to create a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res2 = pt2.CreateUser(username);
                if(res2 != -2)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
                pt2?.Dispose();
            }
        }


        //  Test_1_4_1  Verify that we can change a test user's password.
        [TestMethod]
        public void Test_1_4_1()
        {
            Postgres_Tools pt = null;
            Postgres_Tools pt2 = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a non superuser...
                string mortaluser = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res1 = pt.CreateUser(mortaluser, mortaluser_password);
                if(res1 != 1)
                    Assert.Fail("Wrong Value");


                // Verify the test user can login...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser;
                    d.Password = mortaluser_password;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }


                // Attempt to change the test user's password...
                string newpassword = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res2 = pt.ChangeUserPassword(mortaluser, newpassword);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify the test user can login with the new password...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser;
                    d.Password = newpassword;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Delete the test user...
                var res3 = pt.DeleteUser(mortaluser);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
                pt2?.Dispose();
            }
        }

        //  Test_1_4_2  Verify that a non superuser cannot change a test user's password.
        [TestMethod]
        public void Test_1_4_2()
        {
            Postgres_Tools pt = null;
            Postgres_Tools pt2 = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create test user 1...
                string mortaluser1 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res1 = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(res1 != 1)
                    Assert.Fail("Wrong Value");

                // Create test user 2...
                string mortaluser2 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser2_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res2 = pt.CreateUser(mortaluser2, mortaluser2_password);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");


                // Verify test user 1 can login...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser1;
                    d.Password = mortaluser1_password;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Verify test user 2 can login...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser2;
                    d.Password = mortaluser2_password;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }


                // Have test user 1 attempt to change the password of test user 2...
                string newpassword2 = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Open a connection as test user 1...
                    var pt1 = new Postgres_Tools();
                    pt1.Hostname = dbcreds.Host;
                    pt1.Database = dbcreds.Database;
                    pt1.Username = mortaluser1;
                    pt1.Password = mortaluser1_password;

                    // Attempt to change test user 2's password...
                    var res1a = pt1.ChangeUserPassword(mortaluser2, newpassword2);
                    if(res1a != -2)
                        Assert.Fail("Wrong Value");

                    pt1.Dispose();
                }

                // Verify test user 2 can still login with their original password...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser2;
                    d.Password = mortaluser2_password;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Verify test user 2 can NOT login with the password that test user 1 created for test user 2...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser2;
                    d.Password = newpassword2;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != -1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Delete test user 1...
                var res4 = pt.DeleteUser(mortaluser1);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Delete test user 2...
                var res5 = pt.DeleteUser(mortaluser2);
                if(res5 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
                pt2?.Dispose();
            }
        }

        //  Test_1_4_3  Verify that a non superuser can change their own password.
        [TestMethod]
        public void Test_1_4_3()
        {
            Postgres_Tools pt = null;
            Postgres_Tools pt2 = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create test user 1...
                string mortaluser1 = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string mortaluser1_password = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res1 = pt.CreateUser(mortaluser1, mortaluser1_password);
                if(res1 != 1)
                    Assert.Fail("Wrong Value");

                // Verify test user 1 can login...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser1;
                    d.Password = mortaluser1_password;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Have test user 1 attempt to change their password...
                string mortaluser1_password2 = NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Open a connection as test user 1...
                    var pt1 = new Postgres_Tools();
                    pt1.Hostname  = dbcreds.Host;
                    pt1.Database = dbcreds.Database;
                    pt1.Username = mortaluser1;
                    pt1.Password = mortaluser1_password;

                    // Attempt to change test user 1's password...
                    var res1a = pt1.ChangeUserPassword(mortaluser1, mortaluser1_password2);
                    if(res1a != 1)
                        Assert.Fail("Wrong Value");

                    pt1.Dispose();
                }

                // Have test user 1 attempt to login with the old password...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser1;
                    d.Password = mortaluser1_password;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != -1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Have test user 1 attempt to login with the new password...
                {
                    // Open a connection as the non-superuser...
                    var d = new Postgres_DAL();
                    d.Hostname = dbcreds.Host;
                    d.Database = dbcreds.Database;
                    d.Username = mortaluser1;
                    d.Password = mortaluser1_password2;

                    // Have the mortal user attempt to connect...
                    var res = d.Test_Connection();
                    if(res != 1)
                        Assert.Fail("Wrong Value");

                    d.Dispose();
                }

                // Delete test user 1...
                var res4 = pt.DeleteUser(mortaluser1);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
                pt2?.Dispose();
            }
        }


        //  Test_1_5_1  Verify that we can check a superuser is actually a superuser.
        [TestMethod]
        public void Test_1_5_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Check that the postgres user is a superuser...
                var res1 = pt.IsSuperUser("postgres");
                if(res1 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_5_2  Verify that we can check a non superuser is not a superuser.
        [TestMethod]
        public void Test_1_5_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user is not a superuser...
                var res1 = pt.IsSuperUser(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res2 = pt.DeleteUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_5_3  Verify that we can promote a user to superuser.
        [TestMethod]
        public void Test_1_5_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user is not a superuser...
                var res1 = pt.IsSuperUser(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Promote the test user...
                var res2 = pt.GrantSuperUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it is now a super user...
                var res3 = pt.IsSuperUser(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res4 = pt.DeleteUser(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_5_4  Verify that we can demote a user from superuser to regular user.
        [TestMethod]
        public void Test_1_5_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user is not a superuser...
                var res1 = pt.IsSuperUser(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Promote the test user...
                var res2 = pt.GrantSuperUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it is now a super user...
                var res3 = pt.IsSuperUser(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Demote the test user...
                var res4 = pt.DenySuperUser(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it is no longer a super user...
                var res5 = pt.IsSuperUser(username);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res6 = pt.DeleteUser(username);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_6_1  Verify that we can check a user has CreateDB.
        [TestMethod]
        public void Test_1_6_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Check that the postgres user has CreateDB...
                var res1 = pt.HasDBCreate("postgres");
                if(res1 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_6_2  Verify that we can check a user does not have CreateDB.
        [TestMethod]
        public void Test_1_6_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user does not have CreateDB...
                var res1 = pt.HasDBCreate(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res2 = pt.DeleteUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_6_3  Verify that we can grant CreateDB to a user.
        [TestMethod]
        public void Test_1_6_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user does not have DBCreate role...
                var res1 = pt.HasDBCreate(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Add DBCreate to the test user...
                var res2 = pt.GrantDBCreate(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has DBCreate...
                var res3 = pt.HasDBCreate(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res4 = pt.DeleteUser(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_6_4  Verify that we can deny CreateDB to a user.
        [TestMethod]
        public void Test_1_6_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user doesn't have DBCreate...
                var res1 = pt.HasDBCreate(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Give the test user DBCreate...
                var res2 = pt.GrantDBCreate(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has DBCreate...
                var res3 = pt.HasDBCreate(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Remove DBCreate from the test user...
                var res4 = pt.DenyDBCreate(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it no longer has DBCreate...
                var res5 = pt.HasDBCreate(username);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res6 = pt.DeleteUser(username);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_7_1  Verify that we can check a user has CreateRole.
        [TestMethod]
        public void Test_1_7_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Check that the postgres user has CreateRole...
                var res1 = pt.HasCreateRole("postgres");
                if(res1 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_7_2  Verify that we can check a user does not have CreateRole.
        [TestMethod]
        public void Test_1_7_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user does not have CreateRole...
                var res1 = pt.HasCreateRole(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res2 = pt.DeleteUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_7_3  Verify that we can grant CreateRole to a user.
        [TestMethod]
        public void Test_1_7_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user does not have CreateRole role...
                var res1 = pt.HasCreateRole(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Add CreateRole to the test user...
                var res2 = pt.GrantCreateRole(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has CreateRole...
                var res3 = pt.HasCreateRole(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res4 = pt.DeleteUser(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_7_4  Verify that we can deny CreateRole to a user.
        [TestMethod]
        public void Test_1_7_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + NanoidDotNet.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user does not have CreateRole...
                var res1 = pt.HasCreateRole(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Give the test user CreateRole...
                var res2 = pt.GrantCreateRole(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has CreateRole...
                var res3 = pt.HasCreateRole(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Remove CreateRole from the test user...
                var res4 = pt.DenyCreateRole(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it no longer has CreateRole...
                var res5 = pt.HasCreateRole(username);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res6 = pt.DeleteUser(username);
                if(res6 != 1)
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

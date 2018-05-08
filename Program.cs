using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Diagnostics;

namespace sql_table_merge
{
    class Program
    {
        public static object Util { get; private set; }

        static void Main(string[] args)
        {
            //Base Report Must Have some form of "Base" in Table Name
            string repColsString = "[";          
            string sqlConnMove = @"";
            string sqlConnBaseline = @"";

            Console.Write("What database has the reports? ");
            string mainDB = Console.ReadLine();
            List<string> connStringSplit = sqlConnMove.Split(';').ToList();
            connStringSplit[1] = "Initial Catalog=" + mainDB;
            string connString = string.Join(";", connStringSplit);
            Console.Write("Enter Shared Name Across All Reports: ");
            // i.e. Report
            string commonName = Console.ReadLine();

            SqlConnection conn = new SqlConnection(connString);
            conn.Open();
            
            //BUILD DB
            Console.Write("Enter the New Database Name: ");
            string DBname = Console.ReadLine();
            using (SqlConnection conn2 = new SqlConnection(sqlConnMove))
            {
                conn2.Open();
                var createDB = conn2.CreateCommand();
                createDB.CommandText = "CREATE DATABASE " + DBname;
                createDB.ExecuteNonQuery();
                Console.WriteLine("DB " + DBname + " created.");
            }

            //string buildDB = "CREATE DATABASE " + DBname + " CONTAINMENT = NONE ON PRIMARY (NAME = N'" + DBname + @"', FILENAME = N'D:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\DATA\"
            //    + DBname + ".mdf', SIZE = 39936KB , FILEGROWTH = 10%)";
            //SqlCommand cmdBld = new SqlCommand(buildDB, conn2);
            //cmdBld.ExecuteNonQuery();

            List<string> tables = new List<string>();
            DataTable t = conn.GetSchema("Tables");

            Console.Write("Enter Destination Combined Table Name: ");
            string destinationTableName = Console.ReadLine();
            //string commonName = "Report";
            string baseReport = "";

            foreach (DataRow row in t.Rows)
            {
                string tablename = (string)row[2];
                if (tablename.Contains(commonName)) { tables.Add(tablename); }
                if (tablename.ToLower().Contains("base")) { baseReport = tablename; };
                if (tables.Contains(baseReport)) { tables.Remove(baseReport); }
                //Console.WriteLine(tablename);
            }

            //ADD BASE REPORT TO NEW DB
            String insertBaseToDb = String.Format("SELECT * INTO [{0}].[dbo].[{1}] " +
                "FROM [XM_Financial_Baseline].[dbo].[{2}];", DBname ,destinationTableName, baseReport);
            using (SqlCommand addBaseReport = new SqlCommand(insertBaseToDb, conn))
            {
                addBaseReport.ExecuteNonQuery();
                Console.WriteLine("Base Report: " + baseReport + " added." );
            }

            //tables.RemoveRange(0, 3);    /*FOR TROUBLESHOOTING*/

            foreach (var insertTable in tables)
            {
                string colQuery = String.Format("SELECT * FROM [XM_Financial_Baseline].[dbo].[{0}]", insertTable );
                using (conn = new SqlConnection(sqlConnBaseline))
                {
                    
                    conn.Open();
                    //Console.WriteLine("State" + conn.State);
                    //Console.WriteLine("Timeout" + conn.ConnectionTimeout);

                    using (SqlCommand cmd = new SqlCommand(colQuery, conn))
                    {
                        try
                        {
                            SqlDataReader dr = cmd.ExecuteReader();
                            cmd.CommandTimeout = 45;
                            while (dr.Read())
                            {
                                var columns = new List<string>();

                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    string colName = dr.GetName(i);
                                    columns.Add(dr.GetName(i));
                                    repColsString += colName;
                                }

                                repColsString = String.Join("],[", columns);
                                repColsString = "[" + repColsString + "]";
                                //Console.WriteLine(repColsString);
                                conn.Close();
                                dr.Close();
                                conn.Open();
                                
                                using (SqlCommand insCom = conn.CreateCommand())
                                {
                                    insCom.CommandType = CommandType.Text;
                                    string insertQuery = "INSERT INTO [" + DBname + " ].[dbo].["+ destinationTableName + "] (" + repColsString + ") " +
                                        "SELECT " + repColsString + " FROM [XM_Financial_Baseline].[dbo].[" + insertTable + "];";

                                    //Console.WriteLine(insertQuery);
                                    insCom.CommandText = insertQuery;
                                    //insCom.Parameters.Add(new SqlParameter("@COLUMNS", "works"));

                                    insCom.ExecuteNonQuery();
                                    Console.WriteLine("\n" + insertTable + " added. ");
                                }
                                
                                break;
                            }

                        }
                        catch (Exception ex)
                        {
                            StringBuilder sb = new StringBuilder();
                            Console.WriteLine(sb.Append(ex.Message) + " on table " + insertTable + ".\n");
                        }
                    }
                }
            }

            Console.WriteLine("Completed All Reports");
            Console.ReadKey();
            conn.Close();
        }
    }
}

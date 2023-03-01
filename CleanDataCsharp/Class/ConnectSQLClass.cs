using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace CleanDataCsharp.Class
{
    public class ConnectSQLClass
    {
        protected readonly IConfiguration Configuration;
        SecurityClass Security = new SecurityClass();
        SqlConnectionStringBuilder ConStringBuilder = new();

        public SqlConnection conectionSQL()
        {
            try
            {
                /*
                 "srvr": "cwByAHYAcgBkAGIAcwBxAGwAdgBlAG4AdABhAHMALgBkAGEAdABhAGIAYQBzAGUALgB3AGkAbgBkAG8AdwBzAC4AbgBlAHQA",
        "db": "VgBlAG4AdABhAHMARQBUAEwA",
        "us": "cwBhAHMAcQBsAA==",
        "pwd": "SABvAGwAYQAxADIAMwArAA=="
                 */
                ConStringBuilder.DataSource = Security.DesEncriptar("cwByAHYAcgBkAGIAcwBxAGwAdgBlAG4AdABhAHMALgBkAGEAdABhAGIAYQBzAGUALgB3AGkAbgBkAG8AdwBzAC4AbgBlAHQA");
                ConStringBuilder.InitialCatalog = Security.DesEncriptar("dABlAHMAdABzAHEAbAB2AGUAbgB0AGEAcwA=");
                ConStringBuilder.IntegratedSecurity = false;
                ConStringBuilder.UserID = Security.DesEncriptar("cwBhAHMAcQBsAA==");
                ConStringBuilder.Password = Security.DesEncriptar("SABvAGwAYQAxADIAMwArAA==");
                ConStringBuilder.ConnectTimeout = 60;
                var cssql = ConStringBuilder.ConnectionString;
                SqlConnection connection = new SqlConnection(cssql);
                return connection;
            }
            catch (Exception ex)
            {
                throw;
            }

        }
        public SqlCommand Comando()
        {
            SqlConnection CommandConnection = conectionSQL();
            CommandConnection.Open();
            SqlCommand ExecCommand = CommandConnection.CreateCommand();          
            return ExecCommand;
        }
        public DataTable GetTable_SPSQL(string SP = "", int Bandera = 0, string TableName = "")
        {
            DataTable DataResult = new DataTable();
            SqlCommand ExecCommand = Comando();
            ExecCommand.CommandType = System.Data.CommandType.StoredProcedure;
            ExecCommand.CommandText = SP;
            ExecCommand.Parameters.Add("@BANDERA", SqlDbType.Int).Value = Bandera;
            ExecCommand.Parameters.Add("@TABLENAME", SqlDbType.VarChar).Value = TableName;
            DataResult.TableName = SP;
            try
            {
                DataResult.Load(ExecCommand.ExecuteReader());                
            }
            catch (Exception ex)
            {
                DataResult.TableName = "Exception";
                DataResult.Columns.Add("ERROR");
                DataRow row = DataResult.NewRow();
                row["ERROR"] = ex.Message + "_" + ex.InnerException;
                DataResult.Rows.Add(row);
            }
            return DataResult;
        }
    }
}

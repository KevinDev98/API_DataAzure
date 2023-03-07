using Microsoft.Extensions.Configuration;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace CleanDataCsharp.Class
{
    public class ConnectSQLClass
    {
        //protected readonly IConfiguration _Configuration;
        public IConfiguration _Configuration;
        SecurityClass Security = new SecurityClass();
        SqlConnectionStringBuilder ConStringBuilder = new();        
        public SqlConnection conectionSQL()
        {
            try
            {
                _Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
                var SQLS = _Configuration.GetSection("ConnectionStrings").Get<SQL>();

                ConStringBuilder.DataSource = Security.DesEncriptar(SQLS.srvr);
                ConStringBuilder.InitialCatalog = Security.DesEncriptar(SQLS.db);
                ConStringBuilder.IntegratedSecurity = false;
                ConStringBuilder.UserID = Security.DesEncriptar(SQLS.us);
                ConStringBuilder.Password = Security.DesEncriptar(SQLS.pwd);
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
        public DataTable GetTable_SPSQL(string SP = "", int Bandera = 0, string SchemaName = "", string TableName = "")
        {
            DataTable DataResult = new DataTable();
            try
            {
                SqlCommand ExecCommand = Comando();
                ExecCommand.CommandType = System.Data.CommandType.StoredProcedure;
                ExecCommand.CommandText = SP;
                ExecCommand.Parameters.Add("@BANDERA", SqlDbType.Int).Value = Bandera;
                ExecCommand.Parameters.Add("@SCHEMANAME", SqlDbType.VarChar).Value = SchemaName;
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

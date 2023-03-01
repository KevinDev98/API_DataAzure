using Azure;
using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Net;

namespace CleanDataCsharp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SQLController : ControllerBase
    {
        FunctionsClass Functions = new FunctionsClass();
        AzureFunctionsClass FAzure; // = new AzureFunctionsClass();
        ResponsesModel jsonresponse = new ResponsesModel();
        ConnectSQLClass sqls = new ConnectSQLClass();
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        Boolean TorF = false;
        JsonResult jsonR;

        [HttpPost]
        [Route("GetDataSQL")]
        public string GetDataSQL(GetSQLModel parametros)
        {
            DT_DataSource = sqls.GetTable_SPSQL("SP_GET_TABLE", 0, parametros.TableName);
            if (DT_DataSource.Columns[0].ColumnName != "ERROR")
            {
                TorF = true;
            }
            if (TorF == false)
            {
                jsonresponse.CodeResponse = 400;
                jsonresponse.MessageResponse = "error consultando los datos SQL: " + DT_DataSource.Rows[0][0].ToString;
            }
            else
            {
                try
                {
                    FAzure = new AzureFunctionsClass(parametros.contenedor, parametros.key);
                    DT_DataSource = Functions.CleanDataTable(DT_DataSource);
                    string url = FAzure.GetUrlContainer();
                    FAzure.FromSQLtoBlobDLSG2(url, parametros.StrFileName, DT_DataSource);
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "archivo: " + url + parametros.StrFileName;
                }
                catch (Exception ex)
                {
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "error en el proceso SQL: " + ex.Message + "_" + ex.InnerException;
                }
                jsonresponse.CodeResponse = 200;
            }
            return jsonresponse.MessageResponse;
        }
    }
}

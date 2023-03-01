using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using Microsoft.AspNetCore.Mvc;
using System.Data;

namespace CleanDataCsharp.Controllers
{
    [ApiController]
    [Route("Azure")]
    public class SQLdataController : Controller
    {
        FunctionsClass Functions = new FunctionsClass();
        AzureFunctionsClass FAzure;
        ResponsesModel jsonresponse = new ResponsesModel();
        ConnectSQLClass sqls = new ConnectSQLClass();
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        Boolean TorF = false;
        JsonResult jsonR;

        [HttpPost]
        [Route("GetDataSQL")]
        public IActionResult GetDataSQL(GetSQLModel parametros)
        {
            DT_DataSource = sqls.GetTable_SPSQL("SP_GET_TABLE", 0, parametros.TableName);
            if (!DT_DataSource.Columns[0].ColumnName.Contains("ERROR"))
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
                    DT_DataSource = Functions.DropDuplicates(DT_DataSource);
                    DT_DataSource = Functions.CleanDataTable(DT_DataSource);                    
                    if (!DT_DataSource.Columns[0].ColumnName.Contains("ERROR"))
                    {
                        DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
                        //dataerror = Functions.GetDTErrores();
                        FAzure = new AzureFunctionsClass(parametros.contenedor, parametros.key);                        
                        string url = FAzure.GetUrlContainer();
                        FAzure.FromSQLtoBlobDLSG2(url, parametros.StrFileName, DT_DataSource);
                        jsonresponse.CodeResponse = 200;
                        jsonresponse.MessageResponse = "archivoprocesado correctamente: " + url + parametros.StrFileName;
                    }
                    else
                    {
                        jsonresponse.CodeResponse = 400;
                        jsonresponse.MessageResponse = "error procesando datos SQL: " + DT_DataSource.Rows[0][0].ToString;
                    }

                }
                catch (Exception ex)
                {
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "error en el proceso SQL: " + ex.Message + "_" + ex.InnerException;
                }

            }
            return Json(jsonresponse);
        }
    }
}

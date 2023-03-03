using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Net;
using System.Security.Claims;

namespace CleanDataCsharp.Controllers
{
    [ApiController]
    [Route("SQL")]
    public class SQLdataController : Controller
    {
        FunctionsClass Functions = new FunctionsClass();
        AzureFunctionsClass FAzure;
        ResponsesModel jsonresponse = new ResponsesModel();
        ConnectSQLClass sqls = new ConnectSQLClass();
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        DataTable DataValidate = new DataTable();
        Boolean TorF = false;
        int errorproceso;
        DataTable Process = new DataTable();
        String name;

        public IConfiguration _Configuration;
        Jwt token = new Jwt();
        string solicitante;
        int usrexists = 0;

        [HttpPost]
        [Route("GetDataSQL")]
        public IActionResult GetDataSQL(SQLDataModel parametros)
        {
            if (string.IsNullOrEmpty(parametros.contenedor) || string.IsNullOrEmpty(parametros.TableName) || string.IsNullOrEmpty(parametros.StrFileName) || string.IsNullOrEmpty(parametros.key))
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "parametros vacios";
            }
            else
            {
                DataValidate = new DataTable();
                DataValidate.Columns.Add("Status code");
                DataValidate.Columns.Add("Archivo");
                DataValidate.Columns.Add("resultado");
                DataValidate.Columns.Add("URL");

                _Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
                var sqlsConf = _Configuration.GetSection("ConnectionStrings").Get<SQL>();
                for (int z = 0; z < sqlsConf.UsuarioSolicitante.Count; z++)
                {
                    solicitante = sqlsConf.UsuarioSolicitante[z];
                    if (parametros.usuarioemail == solicitante)
                    {
                        usrexists = 1;
                        break;
                    }
                }
                var identity = HttpContext.User.Identity as ClaimsIdentity;
                var resulttoken = token.ValidateTokenAzDL(identity);
                if (!resulttoken.success)
                {
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = resulttoken.result;
                    return Json(jsonresponse);
                }
                if (usrexists == 0)
                {
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "usuario no valido";
                    return Json(jsonresponse);
                }
                else
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
                            dataerror = Functions.GetDTErrores();
                            string url, Upload, limpios, sucios;
                            HttpStatusCode statusCode;
                            
                            if (!DT_DataSource.Columns[0].ColumnName.Contains("ERROR"))
                            {
                                if (DT_DataSource.Rows.Count > 0)
                                {
                                    FAzure = new AzureFunctionsClass(parametros.contenedor);
                                    Process = DT_DataSource;
                                    name = parametros.StrFileName;
                                    url = FAzure.GetUrlContainer();
                                    Upload = FAzure.FromSQLtoBlobDLSG2(name, Process);
                                    if (Upload.ToLower().Contains("error"))
                                    {
                                        errorproceso = 1;
                                        statusCode = HttpStatusCode.BadRequest;
                                        limpios = "error cargando el archivo" + Upload;
                                        jsonresponse.CodeResponse = 400;
                                        jsonresponse.MessageResponse = "No se completaron todos los procesos";
                                    }
                                    else
                                    {
                                        errorproceso = 0;
                                        statusCode = HttpStatusCode.OK;
                                        limpios = DT_DataSource.Rows.Count.ToString() + " Datos procesados correctamente";
                                        jsonresponse.CodeResponse = 200;
                                        jsonresponse.MessageResponse = "Se completaron todos los procesos";
                                    }
                                    DataValidate.Rows.Add(statusCode.ToString(), name, limpios, url + name);
                                }
                            }
                            else
                            {
                                FAzure = new AzureFunctionsClass(parametros.contenedorRejec);
                                url = FAzure.GetUrlContainer();
                                Upload = FAzure.FromSQLtoBlobDLSG2(parametros.StrFileName, DT_DataSource);
                                if (Upload.ToLower().Contains("error"))
                                {
                                    errorproceso = 1;
                                    statusCode = HttpStatusCode.BadRequest;
                                    sucios = "error cargando el archivo" + Upload;
                                    jsonresponse.MessageResponse = "ERROR EN CARGANDP EL ARCHIVO: " + url + "rejectedSQL_Table_" + parametros.StrFileName;
                                    jsonresponse.CodeResponse = 400;
                                    jsonresponse.MessageResponse = "No se completaron todos los procesos";
                                    DataValidate.Rows.Add(statusCode.ToString(), name, jsonresponse.MessageResponse, "--");
                                }
                                else
                                {
                                    errorproceso = 0;
                                    jsonresponse.CodeResponse = 400;
                                    statusCode = HttpStatusCode.OK;
                                    jsonresponse.MessageResponse = "ERROR EN EL PROCESO: " + url + "rejectedSQL_Table_" + parametros.StrFileName;
                                    DataValidate.Rows.Add(statusCode.ToString(), name, jsonresponse.MessageResponse, url + "rejectedSQL_Table_" + name);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            jsonresponse.CodeResponse = 400;
                            jsonresponse.MessageResponse = "error en el proceso SQL: " + ex.Message + "_" + ex.InnerException;
                        }
                        jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                    }
                }
            }
            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("TransformDataSQL")]
        public IActionResult TransformDataSQL(SQLDataModel parametros)
        {
            if (string.IsNullOrEmpty(parametros.contenedorRejec) || string.IsNullOrEmpty(parametros.contenedor) || string.IsNullOrEmpty(parametros.TableName) || string.IsNullOrEmpty(parametros.StrFileName) || string.IsNullOrEmpty(parametros.key))
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "parametros vacios";
            }
            else
            {
                DataValidate = new DataTable();
                DataValidate.Columns.Add("Status code");
                DataValidate.Columns.Add("Archivo");
                DataValidate.Columns.Add("resultado");
                DataValidate.Columns.Add("URL");

                _Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
                var sqlsConf = _Configuration.GetSection("ConnectionStrings").Get<SQL>();
                for (int z = 0; z < sqlsConf.UsuarioSolicitante.Count; z++)
                {
                    solicitante = sqlsConf.UsuarioSolicitante[z];
                    if (parametros.usuarioemail == solicitante)
                    {
                        usrexists = 1;
                        break;
                    }
                }
                var identity = HttpContext.User.Identity as ClaimsIdentity;
                var resulttoken = token.ValidateTokenAzDL(identity);
                if (!resulttoken.success)
                {
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = resulttoken.result;
                    return Json(jsonresponse);
                }
                if (usrexists == 0)
                {
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "usuario no valido";
                    return Json(jsonresponse);
                }
                else
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
                            DT_DataSource = Functions.CleanDataTableSQL(DT_DataSource);
                            DT_DataSource = Functions.DeleteDirtyRowSQL(DT_DataSource);
                            dataerror = Functions.GetDTErrores();
                            string url, Upload, limpios, sucios;
                            HttpStatusCode statusCode;
                            
                            if (!DT_DataSource.Columns[0].ColumnName.Contains("ERROR"))
                            {
                                if (DT_DataSource.Rows.Count > 0)
                                {
                                    FAzure = new AzureFunctionsClass(parametros.contenedor);
                                    Process = DT_DataSource;
                                    name = parametros.StrFileName;
                                    url = FAzure.GetUrlContainer();
                                    Upload = FAzure.FromSQLtoBlobDLSG2(name, Process);
                                    if (Upload.ToLower().Contains("error"))
                                    {
                                        errorproceso = 1;
                                        statusCode = HttpStatusCode.BadRequest;
                                        limpios = "error cargando el archivo" + Upload;
                                        jsonresponse.CodeResponse = 400;
                                        jsonresponse.MessageResponse = "No se completaron todos los procesos";
                                        DataValidate.Rows.Add(statusCode.ToString(), name, limpios, "--");
                                    }
                                    else
                                    {
                                        errorproceso = 0;
                                        statusCode = HttpStatusCode.OK;
                                        limpios = DT_DataSource.Rows.Count.ToString() + " Datos procesados correctamente";
                                        jsonresponse.CodeResponse = 200;
                                        jsonresponse.MessageResponse = "Se completaron todos los procesos";
                                        DataValidate.Rows.Add(statusCode.ToString(), name, limpios, url + name);
                                    }
                                }
                                if (dataerror.Rows.Count > 0)
                                {
                                    FAzure = new AzureFunctionsClass(parametros.contenedorRejec);
                                    Process = dataerror;
                                    name = "rejectedSQL_Table_" + parametros.StrFileName;
                                    url = FAzure.GetUrlContainer();
                                    Upload = FAzure.FromSQLtoBlobDLSG2(name, Process);
                                    if (Upload.ToLower().Contains("error"))
                                    {
                                        errorproceso = 1;
                                        statusCode = HttpStatusCode.BadRequest;
                                        sucios = "error cargando el archivo" + Upload;
                                        jsonresponse.CodeResponse = 400;
                                        jsonresponse.MessageResponse = "No se completaron todos los procesos";
                                        DataValidate.Rows.Add(statusCode.ToString(), name, sucios, "--");
                                    }
                                    else
                                    {
                                        errorproceso = 0;
                                        statusCode = HttpStatusCode.OK;
                                        sucios = dataerror.Rows.Count.ToString() + " Datos procesados correctamente";
                                        jsonresponse.CodeResponse = 400;
                                        jsonresponse.MessageResponse = "Se completaron todos los procesos";
                                        DataValidate.Rows.Add(statusCode.ToString(), name, sucios, url + name);
                                    }
                                }
                            }
                            else
                            {
                                FAzure = new AzureFunctionsClass(parametros.contenedorRejec);
                                url = FAzure.GetUrlContainer();
                                Upload = FAzure.FromSQLtoBlobDLSG2(parametros.StrFileName, DT_DataSource);
                                if (Upload.ToLower().Contains("error"))
                                {
                                    errorproceso = 1;
                                    statusCode = HttpStatusCode.BadRequest;
                                    sucios = "error cargando el archivo" + Upload;
                                    jsonresponse.MessageResponse = "ERROR EN CARGANDP EL ARCHIVO: " + url + "rejectedSQL_Table_" + parametros.StrFileName;
                                    jsonresponse.CodeResponse = 400;
                                    jsonresponse.MessageResponse = "No se completaron todos los procesos";
                                    DataValidate.Rows.Add(statusCode.ToString(), name, jsonresponse.MessageResponse, "--");
                                }
                                else
                                {
                                    errorproceso = 0;
                                    jsonresponse.CodeResponse = 400;
                                    statusCode = HttpStatusCode.OK;
                                    jsonresponse.MessageResponse = "ERROR EN EL PROCESO: " + url + "rejectedSQL_Table_" + parametros.StrFileName;
                                    DataValidate.Rows.Add(statusCode.ToString(), name, jsonresponse.MessageResponse, url + "rejectedSQL_Table_" + name);
                                }
                                //jsonresponse.CodeResponse = 400;
                                //jsonresponse.MessageResponse = "error procesando datos SQL: " + DT_DataSource.Rows[0][0].ToString;
                            }

                        }
                        catch (Exception ex)
                        {
                            jsonresponse.CodeResponse = 400;
                            jsonresponse.MessageResponse = "error en el proceso SQL: " + ex.Message + "_" + ex.InnerException;
                        }
                        jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                    }
                }
            }
            return Json(jsonresponse);
        }
    }
}

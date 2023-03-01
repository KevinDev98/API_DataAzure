using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Net;

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
        DataTable DataValidate = new DataTable();
        Boolean TorF = false;
        int errorproceso;
        DataTable Process = new DataTable();
        String name;

        [HttpPost]
        [Route("TransformDataSQL")]
        public IActionResult TransformDataSQL(GetSQLModel parametros)
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
                    DataValidate.Columns.Add("Status code");
                    DataValidate.Columns.Add("Archivo");
                    DataValidate.Columns.Add("resultado");
                    DataValidate.Columns.Add("URL");
                    if (!DT_DataSource.Columns[0].ColumnName.Contains("ERROR"))
                    {
                        if (DT_DataSource.Rows.Count > 0)
                        {
                            FAzure = new AzureFunctionsClass(parametros.contenedor, parametros.key);
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
                            FAzure = new AzureFunctionsClass(parametros.contenedorRejec, parametros.key);
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
                                DataValidate.Rows.Add(statusCode.ToString(), name, sucios, url + "rejectedSQL_Table_" + name);
                            }                            
                        }
                    }
                    else
                    {
                        FAzure = new AzureFunctionsClass(parametros.contenedorRejec, parametros.key);
                        url = FAzure.GetUrlContainer();
                        Upload=FAzure.FromSQLtoBlobDLSG2(parametros.StrFileName, DT_DataSource);
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
            return Json(jsonresponse);
        }

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
                    dataerror = Functions.GetDTErrores();
                    string url, Upload, limpios, sucios;
                    HttpStatusCode statusCode;
                    DataValidate.Columns.Add("Status code");
                    DataValidate.Columns.Add("Archivo");
                    DataValidate.Columns.Add("resultado");
                    DataValidate.Columns.Add("URL");
                    if (!DT_DataSource.Columns[0].ColumnName.Contains("ERROR"))
                    {
                        if (DT_DataSource.Rows.Count > 0)
                        {
                            FAzure = new AzureFunctionsClass(parametros.contenedor, parametros.key);
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
                        FAzure = new AzureFunctionsClass(parametros.contenedorRejec, parametros.key);
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
            return Json(jsonresponse);
        }
    }
}

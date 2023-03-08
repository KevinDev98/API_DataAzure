using Microsoft.AspNetCore.Mvc;
using Azure.Storage.Files.DataLake;
using Azure.Storage;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System;
using System.Reflection;
using Azure.Storage.Blobs;
//using System.Xml.Linq;
using System.IO;
using System.Net;
using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using static System.Net.Mime.MediaTypeNames;
using System.ComponentModel;
using System.Security.Claims;
using Microsoft.Extensions.Configuration;

namespace CleanDataCsharp.Controllers
{
    [ApiController]
    [Route("Azure")]
    public class AzureBlobFilesController : Controller
    {
        FunctionsClass Functions = new FunctionsClass();
        AzureFunctionsClass Azure; // = new AzureFunctionsClass();
        ResponsesModel jsonresponse = new ResponsesModel();
        SecurityClass Security = new SecurityClass();

        DataTable DataValidate = new DataTable();
        Boolean TorF = false;
        string rutaOutput = "";
        string FileName = "";
        int errorproceso = 0;
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        string Contenedor, raw, transformed, curated, rejected;
        List<string> NombresArchivos = new List<string>();

        public IConfiguration _Configuration;
        Jwt token = new Jwt();
        string solicitante;
        int usrexists = 0;

        [HttpPost]
        [Route("DataStandar")]
        public IActionResult Estandarizacion(RawModel parametros) //from raw to clean
        {
            try
            {
                DataValidate = new DataTable();
                DataValidate.Columns.Add("Status code");
                DataValidate.Columns.Add("Archivo Trabajado");
                DataValidate.Columns.Add("URL Archivo");
                if (string.IsNullOrEmpty(parametros.ContenedorRAW) || string.IsNullOrEmpty(parametros.ContenedorIngesta) || parametros.NombresArchivosN.Count == 0)
                {
                    errorproceso = 1;
                    //jsonresponse.Response = response;
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    _Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
                    var Az = _Configuration.GetSection("AzureConf").Get<AzureCon>();
                    for (int z = 0; z < Az.AplicantsemailAddress.Count; z++)
                    {
                        solicitante = Az.AplicantsemailAddress[z];
                        if (parametros.usuarioemail == solicitante)
                        {
                            usrexists = 1;
                            break;
                        }
                    }
                    //var identity = HttpContext.User.Identity as ClaimsIdentity;
                    //var resulttoken = token.ValidateTokenAzDL(identity);
                    //if (!resulttoken.success)
                    //{
                    //    jsonresponse.CodeResponse = 400;
                    //    jsonresponse.MessageResponse = resulttoken.message;
                    //    return Json(jsonresponse);
                    //}
                    if (usrexists == 0)
                    {
                        jsonresponse.CodeResponse = 400;
                        jsonresponse.MessageResponse = "usuario no valido";
                        return Json(jsonresponse);
                    }
                    else
                    {
                        Contenedor = parametros.ContenedorIngesta;
                        raw = parametros.ContenedorRAW;
                        NombresArchivos = parametros.NombresArchivosN;
                        Azure = new AzureFunctionsClass(Contenedor);

                        if (NombresArchivos.Count == 1)
                        {
                            rutaOutput = Azure.GetUrlContainer();
                            FileName = NombresArchivos[0];
                            if (FileName == "*")
                            {
                                NombresArchivos = Azure.ListFile(rutaOutput, Contenedor);
                            }
                        }
                        for (int k = 0; k < NombresArchivos.Count; k++)// este for se deja con un valor en duro, ya que para este ejercicio solo se cuentan con 3 archivos
                        {
                            DT_DataSource = new DataTable();
                            dataerror = new DataTable();

                            FileName = NombresArchivos[k];
                            DT_DataSource = Azure.TransformFileforAzure(FileName, parametros.delimitador);
                            if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                            {
                                errorproceso = 1;
                                DataValidate.Rows.Add(HttpStatusCode.NotFound.ToString(), FileName, DT_DataSource.Rows[0][0].ToString());
                            }
                            else
                            {
                                DT_DataSource = Functions.DropDuplicates(DT_DataSource);
                                try
                                {
                                    dataerror = Functions.GetDTErrores();
                                    string limpios, Upload;
                                    string URL = "";
                                    string Ext="";
                                    //FileName = FileName.Replace("Clean", "");
                                    if (FileName.Contains(".csv"))
                                    {
                                        FileName = FileName.Replace(".csv", "");
                                        Ext = "csv_";
                                    }
                                    else if (FileName.Contains(".json"))
                                    {
                                        FileName = FileName.Replace(".json", "");
                                        Ext = "json_";
                                    }
                                    else if (FileName.Contains(".xml"))
                                    {
                                        FileName = FileName.Replace(".xml", "");
                                        Ext = "xml_";
                                    }
                                    else if (FileName.Contains(".txt"))
                                    {
                                        FileName = FileName.Replace(".txt", "");
                                        Ext = "txt_";
                                    }
                                    FileName = "Origen_" + Ext + FileName;
                                    if (DT_DataSource.Rows.Count > 0)
                                    {
                                        rutaOutput = Azure.GetUrlContainer();
                                        Upload = Azure.UploadBlobDLSG2(FilenameAz: FileName + ".csv", table: DT_DataSource, ContainerBlobName: raw);
                                        if (Upload.ToLower().Contains("error"))
                                        {
                                            errorproceso = 1;
                                            DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "Error cargando el archivo", Upload);
                                        }
                                        else
                                        {
                                            rutaOutput = Azure.GetUrlContainer();
                                            rutaOutput = rutaOutput.Replace(Contenedor, raw);
                                            URL = rutaOutput + FileName + ".csv";
                                            DataValidate.Rows.Add(HttpStatusCode.OK.ToString(), FileName + ".csv", URL);
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    errorproceso = 1;
                                    jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message + "_" + ex.InnerException;
                                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorproceso = 1;
                jsonresponse.MessageResponse = "Error en el proceso Estandarización: " + ex.Message + "_" + ex.InnerException;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
            }
            if (errorproceso == 0)
            {
                //jsonresponse.Response = response;
                jsonresponse.CodeResponse = 200;
                jsonresponse.MessageResponse = "Proceso Terminado con Exito";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            else
            {
                jsonresponse.CodeResponse = 404;
                jsonresponse.MessageResponse = "No se cargaron todos los archivos";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("DataTransformed")]
        public IActionResult DataTransformed(TransformedModel parametros)
        {
            try
            {
                DataValidate = new DataTable();
                DataValidate.Columns.Add("Status Code");
                DataValidate.Columns.Add("Archivo Trabajado");
                DataValidate.Columns.Add("Archivo transformed");
                DataValidate.Columns.Add("URL transformed");
                DataValidate.Columns.Add("Archivo Rejected");
                DataValidate.Columns.Add("URL Rejected");

                if (string.IsNullOrEmpty(parametros.ContenedorOrigen) || parametros.NombresArchivosN.Count == 0 || string.IsNullOrEmpty(parametros.ContenedorTransformed) || string.IsNullOrEmpty(parametros.ContenedorRejected))
                {
                    errorproceso = 1;
                    //jsonresponse.Response = response;
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    _Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
                    var Az = _Configuration.GetSection("AzureConf").Get<AzureCon>();
                    for (int z = 0; z < Az.AplicantsemailAddress.Count; z++)
                    {
                        solicitante = Az.AplicantsemailAddress[z];
                        if (parametros.usuarioemail == solicitante)
                        {
                            usrexists = 1;
                            break;
                        }
                    }
                    //var identity = HttpContext.User.Identity as ClaimsIdentity;
                    //var resulttoken = token.ValidateTokenAzDL(identity);
                    //if (!resulttoken.success)
                    //{
                    //    jsonresponse.CodeResponse = 400;
                    //    jsonresponse.MessageResponse = resulttoken.result;
                    //    return Json(jsonresponse);
                    //}
                    if (usrexists == 0)
                    {
                        jsonresponse.CodeResponse = 400;
                        jsonresponse.MessageResponse = "usuario no valido";
                        return Json(jsonresponse);
                    }
                    else
                    {
                        Contenedor = parametros.ContenedorOrigen;
                        NombresArchivos = parametros.NombresArchivosN;
                        transformed = parametros.ContenedorTransformed;
                        rejected = parametros.ContenedorRejected;
                        Azure = new AzureFunctionsClass(Contenedor);

                        for (int k = 0; k < NombresArchivos.Count; k++)// este for se deja con un valor en duro, ya que para este ejercicio solo se cuentan con 3 archivos
                        {
                            FileName = NombresArchivos[k];
                            DT_DataSource = new DataTable();
                            try
                            {
                                DT_DataSource = Azure.TransformFileforAzure(FileName);
                                if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                                {
                                    errorproceso = 1;
                                    DataValidate.Rows.Add(HttpStatusCode.NotFound.ToString(), FileName, DT_DataSource.Rows[0][0].ToString(), "Incorrecto", "Incorrecto", "Incorrecto");
                                }
                                else
                                {
                                    DT_DataSource = Functions.DropDuplicates(DT_DataSource);
                                    DT_DataSource = Functions.CleanDataTable(DT_DataSource);
                                    DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
                                    dataerror = Functions.GetDTErrores();
                                    try
                                    {
                                        string limpios, sucios, Upload;
                                        string URLlimpios = "";
                                        string URLsucios = "";
                                        //FileName = FileName.Replace("Clean", "");
                                        if (DT_DataSource.Rows.Count > 0)
                                        {
                                            if (FileName.Contains(".csv"))
                                            {
                                                FileName = FileName.Replace(".csv", "");
                                            }
                                            else if (FileName.Contains(".json"))
                                            {
                                                FileName = FileName.Replace(".json", "");
                                            }
                                            else if (FileName.Contains(".xml"))
                                            {
                                                FileName = FileName.Replace(".xml", "");
                                            }
                                            else if (FileName.Contains(".txt"))
                                            {
                                                FileName = FileName.Replace(".txt", "");
                                            }
                                            if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                                            {
                                                rutaOutput = Azure.GetUrlContainer();
                                                Upload = Azure.UploadBlobDLSG2(FilenameAz: "Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                                if (Upload.ToLower().Contains("error"))
                                                {
                                                    errorproceso = 1;
                                                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, Upload, "Incorrecto", "Incorrecto", "Incorrecto");
                                                }
                                            }
                                            else
                                            {
                                                rutaOutput = Azure.GetUrlContainer();
                                                Upload = Azure.UploadBlobDLSG2(FilenameAz: "transformed_" + FileName + ".csv", table: DT_DataSource, ContainerBlobName: transformed);
                                                if (Upload.ToLower().Contains("error"))
                                                {
                                                    errorproceso = 1;
                                                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, Upload, "Incorrecto", "Incorrecto", "Incorrecto");
                                                }
                                                else
                                                {
                                                    rutaOutput = Azure.GetUrlContainer();
                                                    rutaOutput = rutaOutput.Replace(Contenedor, transformed);
                                                    URLlimpios = rutaOutput + "transformed_" + FileName + ".csv";
                                                    if (dataerror.Rows.Count > 0)
                                                    {
                                                        Upload = Azure.UploadBlobDLSG2(FilenameAz: "Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                                        if (Upload.ToLower().Contains("error"))
                                                        {
                                                            errorproceso = 1;
                                                            DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, Upload, "Incorrecto", "Incorrecto", "Incorrecto");
                                                        }
                                                        else
                                                        {
                                                            //limpios y sucios
                                                            rutaOutput = Azure.GetUrlContainer();
                                                            rutaOutput = rutaOutput.Replace(Contenedor, rejected);
                                                            URLsucios = rutaOutput + "Rejected_" + FileName + ".csv";
                                                            limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
                                                            sucios = "Filas sucuias:" + dataerror.Rows.Count.ToString();
                                                            DataValidate.Rows.Add(HttpStatusCode.OK, FileName, limpios, URLlimpios, sucios, URLsucios);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        //solo limpios
                                                        rutaOutput = Azure.GetUrlContainer();
                                                        rutaOutput = rutaOutput.Replace(Contenedor, rejected);
                                                        URLsucios = "No se encontraron registros sucios";
                                                        limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
                                                        sucios = "Filas sucuias:" + dataerror.Rows.Count.ToString();
                                                        DataValidate.Rows.Add(HttpStatusCode.OK, FileName, limpios, URLlimpios, sucios, URLsucios);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        errorproceso = 1;
                                        //response.StatusCode = HttpStatusCode.BadRequest;
                                        ////jsonresponse.Response = response;
                                        jsonresponse.CodeResponse = 400;
                                        jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message + "_" + ex.InnerException;
                                        DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                errorproceso = 1;
                                //response.StatusCode = HttpStatusCode.BadRequest;
                                ////jsonresponse.Response = response;
                                jsonresponse.CodeResponse = 400;
                                jsonresponse.MessageResponse = "Error en el proceso: " + ex.Message + "_" + ex.InnerException;
                                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorproceso = 1;
                //response.StatusCode = HttpStatusCode.BadRequest;
                ////jsonresponse.Response = response;
                jsonresponse.CodeResponse = 400;
                jsonresponse.MessageResponse = "Error en el proceso Transformed: " + ex.Message + "_" + ex.InnerException;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
            }
            if (errorproceso == 0)
            {
                //response.StatusCode = HttpStatusCode.OK;
                ////jsonresponse.Response = response;
                jsonresponse.CodeResponse = 200;
                jsonresponse.MessageResponse = "Proceso Terminado con Exito";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            else
            {
                jsonresponse.CodeResponse = 404;
                jsonresponse.MessageResponse = "No se cargaron todos los archivos";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("RemoveBlobs")]
        public IActionResult RemoveBlobs(RemoveModel parametros)
        {
            string remove = "";
            DataValidate = new DataTable();
            DataValidate.Columns.Add("Status code");
            DataValidate.Columns.Add("Archivo Trabajado");
            DataValidate.Columns.Add("URL Archivo");
            try
            {
                if (string.IsNullOrEmpty(parametros.Contenedor) || parametros.Listfilename.Count == 0)
                {
                    errorproceso = 1;
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Parametros vacios";
                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "vacio", jsonresponse.MessageResponse);
                }
                else
                {
                    _Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
                    var Az = _Configuration.GetSection("AzureConf").Get<AzureCon>();
                    for (int z = 0; z < Az.AplicantsemailAddress.Count; z++)
                    {
                        solicitante = Az.AplicantsemailAddress[z];
                        if (parametros.usuarioemail == solicitante)
                        {
                            usrexists = 1;
                            break;
                        }
                    }
                    //var identity = HttpContext.User.Identity as ClaimsIdentity;
                    //var resulttoken = token.ValidateTokenAzDL(identity);
                    //if (!resulttoken.success)
                    //{
                    //    jsonresponse.CodeResponse = 400;
                    //    jsonresponse.MessageResponse = resulttoken.result;
                    //    return Json(jsonresponse);
                    //}
                    if (usrexists == 0)
                    {
                        jsonresponse.CodeResponse = 400;
                        jsonresponse.MessageResponse = "usuario no valido";
                        return Json(jsonresponse);
                    }
                    else
                    {
                        Contenedor = parametros.Contenedor;
                        NombresArchivos = parametros.Listfilename;
                        Azure = new AzureFunctionsClass(Contenedor);
                        if (NombresArchivos.Count == 1)
                        {
                            rutaOutput = Azure.GetUrlContainer();
                            FileName = NombresArchivos[0];
                            if (FileName == "*")
                            {
                                NombresArchivos = Azure.ListFile(rutaOutput, Contenedor);
                            }
                        }
                        rutaOutput = Azure.GetUrlContainer();
                        for (int k = 0; k < NombresArchivos.Count; k++)
                        {
                            FileName = NombresArchivos[k];
                            remove = Azure.RemoveFiles(FilenameAz: FileName, ContainerBlobName: Contenedor);
                            if (remove.ToLower().Contains("error"))
                            {
                                errorproceso = 1;
                                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "Error eliminando el archivo", remove);
                            }
                            else
                            {
                                DataValidate.Rows.Add(HttpStatusCode.OK.ToString(), FileName, remove);
                            }
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                errorproceso = 1;
                jsonresponse.MessageResponse = "Error en el proceso removeblobs: " + ex.Message + "_" + ex.InnerException;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
            }
            if (errorproceso == 0)
            {
                //response.StatusCode = HttpStatusCode.OK;
                ////jsonresponse.Response = response;
                jsonresponse.CodeResponse = 200;
                jsonresponse.MessageResponse = "Proceso Terminado con Exito. Archivos eliminados";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            else
            {
                jsonresponse.CodeResponse = 404;
                jsonresponse.MessageResponse = "No se eliminaron todos los archivos";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("MoveBlobs")]
        public IActionResult MoveBlobs(ParametrosModel parametros)
        {
            DataValidate = new DataTable();
            DataValidate.Columns.Add("Status code");
            DataValidate.Columns.Add("Archivo Trabajado");
            DataValidate.Columns.Add("URL Archivo");
            try
            {
                if (string.IsNullOrEmpty(parametros.ContenedorOrigen) || string.IsNullOrEmpty(parametros.ContenedorDestino) || parametros.NombresArchivos.Count == 0 || string.IsNullOrEmpty(parametros.usuarioemail))
                {
                    errorproceso = 1;
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Parametros vacios";
                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "vacio", jsonresponse.MessageResponse);
                }
                else
                {
                    string remove, Move;
                    _Configuration = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json")
                    .Build();
                    var Az = _Configuration.GetSection("AzureConf").Get<AzureCon>();
                    for (int z = 0; z < Az.AplicantsemailAddress.Count; z++)
                    {
                        solicitante = Az.AplicantsemailAddress[z];
                        if (parametros.usuarioemail == solicitante)
                        {
                            usrexists = 1;
                            break;
                        }
                    }
                    if (usrexists == 0)
                    {
                        jsonresponse.CodeResponse = 400;
                        jsonresponse.MessageResponse = "usuario no valido";
                        return Json(jsonresponse);
                    }
                    else
                    {
                        NombresArchivos = parametros.NombresArchivos;
                        Azure = new AzureFunctionsClass(Contenedor);
                        if (NombresArchivos.Count == 1)
                        {
                            rutaOutput = Azure.GetUrlContainer();
                            FileName = NombresArchivos[0];
                            if (FileName == "*")
                            {
                                NombresArchivos = Azure.ListFile(rutaOutput, Contenedor);
                            }
                        }
                        rutaOutput = Azure.GetUrlContainer();
                        for (int k = 0; k < NombresArchivos.Count; k++)
                        {
                            FileName = NombresArchivos[k];
                            DT_DataSource = Azure.TransformFileforAzure(FileName);
                            Move = Azure.UploadBlobDLSG2(FilenameAz: FileName, table: DT_DataSource, ContainerBlobName: parametros.ContenedorDestino);
                            if (Move.ToLower().Contains("error"))
                            {
                                errorproceso = 1;
                                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "Error eliminando el archivo", Move);
                            }
                            else
                            {
                                remove = Azure.RemoveFiles(FilenameAz: FileName, ContainerBlobName: parametros.ContenedorOrigen);
                                if (remove.ToLower().Contains("error"))
                                {
                                    errorproceso = 1;
                                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "Error eliminando el archivo", remove);
                                }
                                else
                                {
                                    DataValidate.Rows.Add(HttpStatusCode.OK.ToString(), FileName, remove);
                                }
                            }
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                errorproceso = 1;
                jsonresponse.MessageResponse = "Error en el proceso removeblobs: " + ex.Message + "_" + ex.InnerException;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
            }
            if (errorproceso == 0)
            {
                //response.StatusCode = HttpStatusCode.OK;
                ////jsonresponse.Response = response;
                jsonresponse.CodeResponse = 200;
                jsonresponse.MessageResponse = "Proceso Terminado con Exito. Archivos eliminados";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            else
            {
                jsonresponse.CodeResponse = 404;
                jsonresponse.MessageResponse = "No se eliminaron todos los archivos";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            return Json(jsonresponse);
        }
    }
}

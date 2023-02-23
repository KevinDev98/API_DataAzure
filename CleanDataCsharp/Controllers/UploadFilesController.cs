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

namespace CleanDataCsharp.Controllers
{
    [ApiController]
    [Route("Azure")]
    public class UploadFilesController : Controller
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
        //string Str_Connect = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl98;AccountKey=0Od+makghmoYKNHCBgqUQtlm9t7/0wJQlWZbjkTz8qCJU/QSFITn/TqWTQa/zEkRC33cu0qSWnnv+AStbA4m+Q==;EndpointSuffix=core.windows.net";
        //string Str_Connect2 = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl98;AccountKey=GecQ9fvQOC8fU95LzDE2NRqv7QmXiy+fI4iHMcHE3YVn2KDgwSjxrrxJUjzjMmBNxmoOF38mK+2V+AStB+464w==;EndpointSuffix=core.windows.net";
        string Contenedor, raw, transformed, curated, rejected;
        string ExtencionArchivos;
        List<string> NombresArchivos = new List<string>();
        HttpResponseMessage response = new HttpResponseMessage();

        //[HttpGet]
        //[Route("ExisteClientes")]
        //public IActionResult ExisteClientes(string Container, string File, string Ext)
        //{
        //    Azure = new AzureFunctionsClass(Container, Ext);
        //    DataValidate = Azure.ValidateExistsContainer(File);
        //    //Str_Connect = Security.Encriptar(Str_Connect);
        //    if (DataValidate.Columns[0].ColumnName != "ERROR")
        //    {
        //        TorF = true;
        //    }
        //    try
        //    {
        //        if (TorF)
        //        {
        //            response.StatusCode = HttpStatusCode.BadRequest;
        //            jsonresponse.CodeResponse = 200;
        //            jsonresponse.MessageResponse = "Contenedor " + Container + " y archivo encontrado " + File + "." + Ext + " encontrados";
        //            jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
        //        }
        //        else
        //        {
        //            response.StatusCode = HttpStatusCode.OK;
        //            jsonresponse.CodeResponse = 0;
        //            jsonresponse.MessageResponse = "Contenedor " + Container + " y/o archivo " + File + "." + Ext + " no encontrados";
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        response.StatusCode = HttpStatusCode.BadRequest;
        //        jsonresponse.CodeResponse = 0;
        //        jsonresponse.MessageResponse = "Ocurrio un error en el proceso: " + ex.Message;
        //    }

        //    return Json(jsonresponse);
        //}

        //[HttpPost]
        //[Route("DataCleanVentas")]
        //public IActionResult CleanDataVentas(transformedModel parametros)
        //{
        //    try
        //    {
        //        if (parametros.ContenedorSource == null || parametros.NombresArchivosN.Count == 0 || parametros.ContenedorTransformed == null || parametros.ContenedorRejected == null)
        //        {
        //            jsonresponse.CodeResponse = 0;
        //            jsonresponse.MessageResponse = "Parametros vacios";
        //        }
        //        else
        //        {
        //            Contenedor = parametros.ContenedorSource;
        //            
        //            NombresArchivos = parametros.NombresArchivosN;
        //            transformed = parametros.ContenedorTransformed;
        //            rejected = parametros.ContenedorRejected;
        //            Azure = new AzureFunctionsClass(Contenedor);

        //            DataValidate = new DataTable();
        //            DataValidate.Columns.Add("Archivo Trabajado");
        //            DataValidate.Columns.Add("Archivo transformed");
        //            DataValidate.Columns.Add("URL transformed");
        //            DataValidate.Columns.Add("Archivo Rejected");
        //            DataValidate.Columns.Add("URL Rejected");

        //            for (int k = 0; k < NombresArchivos.Count; k++)// este for se deja con un valor en duro, ya que para este ejercicio solo se cuentan con 3 archivos
        //            {
        //                FileName = NombresArchivos[k];
        //                DT_DataSource = new DataTable();
        //                DT_DataSource = Azure.TransformFileforAzure(FileName);

        //                if (FileName.ToLower().Contains("clientes"))
        //                {
        //                    if (DT_DataSource.Rows.Count > 0)
        //                    {
        //                        dataerror = new DataTable();
        //                        DT_DataSource = Functions.CleanDataTableClientes(DT_DataSource);
        //                    }
        //                }
        //                else if (FileName.ToLower().Contains("productos"))
        //                {
        //                    if (DT_DataSource.Rows.Count > 0)
        //                    {
        //                        dataerror = new DataTable();
        //                        DT_DataSource = Functions.CleanDataTableProductos(DT_DataSource);
        //                    }
        //                }
        //                else if (FileName.ToLower().Contains("Sucursales"))
        //                {
        //                    if (DT_DataSource.Rows.Count > 0)
        //                    {
        //                        dataerror = new DataTable();
        //                        DT_DataSource = Functions.CleanDataTableSucursales(DT_DataSource);
        //                    }
        //                }
        //                else if (FileName.ToLower().Contains("Ventas"))
        //                {
        //                    if (DT_DataSource.Rows.Count > 0)
        //                    {
        //                        dataerror = new DataTable();
        //                        DT_DataSource = Functions.CleanDataTableVentas(DT_DataSource);
        //                    }
        //                }
        //                else
        //                {
        //                    DT_DataSource = Functions.CleanDataTable(DT_DataSource);
        //                }

        //                DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
        //                try
        //                {
        //                    dataerror = Functions.GetDTErrores();
        //                    string limpios, sucios, Upload;
        //                    string URLlimpios = "";
        //                    string URLsucios = "";
        //                    FileName = FileName.Replace("Clean", "");
        //                    int errorproceso = 0;
        //                    if (DT_DataSource.Rows.Count > 0)
        //                    {
        //                        if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
        //                        {
        //                            errorproceso = 1;
        //                            Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
        //                            rutaOutput = Azure.GetUrlContainer();
        //                            rutaOutput = rutaOutput.Replace(Contenedor, rejected);
        //                            URLsucios = rutaOutput + "Rejected_" + FileName + ".csv";
        //                        }
        //                        else
        //                        {
        //                            Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "transformed_" + FileName + ".csv", table: DT_DataSource, ContainerBlobName: transformed);
        //                            rutaOutput = Azure.GetUrlContainer();
        //                            rutaOutput = rutaOutput.Replace(Contenedor, transformed);
        //                            URLlimpios = rutaOutput + "transformed_" + FileName + ".csv";
        //                        }
        //                    }
        //                    if (dataerror.Rows.Count > 0)
        //                    {
        //                        Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
        //                        rutaOutput = Azure.GetUrlContainer();
        //                        rutaOutput = rutaOutput.Replace(Contenedor, rejected);
        //                        URLsucios = rutaOutput + "Rejected_" + FileName + ".csv";
        //                    }
        //                    limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
        //                    if (errorproceso == 1)
        //                    {
        //                        sucios = "Filas sucuias:" + DT_DataSource.Rows.Count.ToString() + ". Archivo original dañado";
        //                    }
        //                    else
        //                    {
        //                        sucios = "Filas sucuias:" + dataerror.Rows.Count.ToString();
        //                    }
        //                    DataValidate.Rows.Add(FileName, limpios, URLlimpios, sucios, URLsucios);
        //                }
        //                catch (Exception ex)
        //                {
        //                    response.StatusCode = HttpStatusCode.BadRequest;
        //                    jsonresponse.CodeResponse = 0;
        //                    jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message;
        //                }
        //            }
        //            response.StatusCode = HttpStatusCode.OK;
        //            jsonresponse.CodeResponse = 200;
        //            jsonresponse.MessageResponse = "Proceso Terminado con Exito";
        //            jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        response.StatusCode = HttpStatusCode.BadRequest;
        //        jsonresponse.CodeResponse = 0;
        //        jsonresponse.MessageResponse = "Error en el proceso CleanData: " + ex.Message;
        //    }
        //    return Json(jsonresponse);
        //}

        [HttpPost]
        [Route("DataTransformed")]
        public IActionResult DataTransformed(CuratedModel parametros)
        {
            try
            {
                if (parametros.ContenedorSource == null || parametros.NombresArchivosN.Count == 0 || parametros.ContenedorTransformed == null || parametros.ContenedorRejected == null)
                {
                    errorproceso = 1;
                    response.StatusCode = HttpStatusCode.BadRequest;
                    jsonresponse.Response = response;
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.ContenedorSource;
                    NombresArchivos = parametros.NombresArchivosN;
                    transformed = parametros.ContenedorTransformed;
                    rejected = parametros.ContenedorRejected;
                    Azure = new AzureFunctionsClass(Contenedor);

                    DataValidate = new DataTable();
                    DataValidate.Columns.Add("Status Code");
                    DataValidate.Columns.Add("Archivo Trabajado");
                    DataValidate.Columns.Add("Archivo transformed");
                    DataValidate.Columns.Add("URL transformed");
                    DataValidate.Columns.Add("Archivo Rejected");
                    DataValidate.Columns.Add("URL Rejected");

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
                                try
                                {
                                    dataerror = Functions.GetDTErrores();
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
                                        if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                                        {
                                            rutaOutput = Azure.GetUrlContainer();
                                            Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                            if (Upload.ToLower().Contains("error"))
                                            {
                                                errorproceso = 1;
                                                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, Upload, "Incorrecto", "Incorrecto", "Incorrecto");
                                            }
                                        }
                                        else
                                        {
                                            rutaOutput = Azure.GetUrlContainer();
                                            Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "transformed_" + FileName + ".csv", table: DT_DataSource, ContainerBlobName: transformed);
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
                                                    Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
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
                                    //jsonresponse.Response = response;
                                    jsonresponse.CodeResponse = 400;
                                    jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message;
                                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            errorproceso = 1;
                            //response.StatusCode = HttpStatusCode.BadRequest;
                            //jsonresponse.Response = response;
                            jsonresponse.CodeResponse = 400;
                            jsonresponse.MessageResponse = "Error en el proceso: " + ex.Message;
                            DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
                        }
                    }
                }
            }
            catch (Exception ex)
            {                
                errorproceso = 1;
                //response.StatusCode = HttpStatusCode.BadRequest;
                //jsonresponse.Response = response;
                jsonresponse.CodeResponse = 400;
                jsonresponse.MessageResponse = "Error en el proceso Transformed: " + ex.Message;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
            }
            if (errorproceso == 0)
            {
                //response.StatusCode = HttpStatusCode.OK;
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
        [Route("DataStandar")]
        public IActionResult Estandarizacion(RawModel parametros) //from raw to clean
        {
            try
            {
                if (parametros.ContenedorRAW == null || parametros.Contenedor == null || parametros.NombresArchivosN.Count == 0)
                {
                    errorproceso = 1;
                    response.StatusCode = HttpStatusCode.BadRequest;
                    jsonresponse.Response = response;
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.Contenedor;
                    raw = parametros.ContenedorRAW;
                    NombresArchivos = parametros.NombresArchivosN;
                    Azure = new AzureFunctionsClass(Contenedor);

                    DataValidate = new DataTable();
                    DataValidate.Columns.Add("Status code");
                    DataValidate.Columns.Add("Archivo Trabajado");
                    DataValidate.Columns.Add("URL Archivo");

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
                        DT_DataSource = Azure.TransformFileforAzure(FileName);
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
                                //FileName = FileName.Replace("Clean", "");
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
                                if (DT_DataSource.Rows.Count > 0)
                                {
                                    rutaOutput = Azure.GetUrlContainer();
                                    Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: FileName + ".csv", table: DT_DataSource, ContainerBlobName: raw);
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
                                        DataValidate.Rows.Add(HttpStatusCode.OK.ToString(), FileName, URL);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                errorproceso = 1;
                                jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message;
                                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorproceso = 1;
                jsonresponse.MessageResponse = "Error en el proceso Estandarización: " + ex.Message;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
            }
            if (errorproceso == 0)
            {
                response.StatusCode = HttpStatusCode.OK;
                jsonresponse.Response = response;
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
                if (parametros.Contenedor == null || parametros.Listfilename.Count == 0)
                {
                    errorproceso = 1;
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Parametros vacios";
                    DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), "vacio", jsonresponse.MessageResponse);
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
                        remove = Azure.RemoveFiles(PathBlob: rutaOutput, FilenameAz: FileName, ContainerBlobName: Contenedor);
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
            catch (Exception ex)
            {
                errorproceso = 1;
                jsonresponse.MessageResponse = "Error en el proceso removeblobs: " + ex.Message;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
            }
            if (errorproceso == 0)
            {
                //response.StatusCode = HttpStatusCode.OK;
                //jsonresponse.Response = response;
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

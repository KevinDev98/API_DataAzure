﻿using Microsoft.AspNetCore.Mvc;
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
        HttpResponseMessage response = new HttpResponseMessage();

        [HttpPost]
        [Route("DataTransformed")]
        public IActionResult DataTransformed(TransformedModel parametros)
        {
            try
            {
                if (parametros.ContenedorSource == null || parametros.NombresArchivosN.Count == 0 || parametros.ContenedorTransformed == null || parametros.ContenedorRejected == null)
                {
                    errorproceso = 1;
                    response.StatusCode = HttpStatusCode.BadRequest;
                    //jsonresponse.Response = response;
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.ContenedorSource;
                    NombresArchivos = parametros.NombresArchivosN;
                    transformed = parametros.ContenedorTransformed;
                    rejected = parametros.ContenedorRejected;
                    Azure = new AzureFunctionsClass(Contenedor, parametros.key);

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
                                        if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                                        {
                                            DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
                                            dataerror = Functions.GetDTErrores();

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
                                    ////jsonresponse.Response = response;
                                    jsonresponse.CodeResponse = 400;
                                    jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message+ "_"+ ex.InnerException;
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
                            jsonresponse.MessageResponse = "Error en el proceso: " + ex.Message+ "_"+ ex.InnerException;
                            DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse, "Incorrecto", "Incorrecto", "Incorrecto");
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
                jsonresponse.MessageResponse = "Error en el proceso Transformed: " + ex.Message+ "_"+ ex.InnerException;
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
        [Route("DataStandar")]
        public IActionResult Estandarizacion(RawModel parametros) //from raw to clean
        {
            try
            {
                if (parametros.ContenedorRAW == null || parametros.Contenedor == null || parametros.NombresArchivosN.Count == 0)
                {
                    errorproceso = 1;
                    response.StatusCode = HttpStatusCode.BadRequest;
                    //jsonresponse.Response = response;
                    jsonresponse.CodeResponse = 400;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.Contenedor;
                    raw = parametros.ContenedorRAW;
                    NombresArchivos = parametros.NombresArchivosN;
                    Azure = new AzureFunctionsClass(Contenedor, parametros.key);

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
                                jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message+ "_"+ ex.InnerException;
                                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errorproceso = 1;
                jsonresponse.MessageResponse = "Error en el proceso Estandarización: " + ex.Message+ "_"+ ex.InnerException;
                DataValidate.Rows.Add(HttpStatusCode.BadRequest.ToString(), FileName, jsonresponse.MessageResponse);
            }
            if (errorproceso == 0)
            {
                response.StatusCode = HttpStatusCode.OK;
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
                    Azure = new AzureFunctionsClass(Contenedor, parametros.key);
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
                jsonresponse.MessageResponse = "Error en el proceso removeblobs: " + ex.Message+ "_"+ ex.InnerException;
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
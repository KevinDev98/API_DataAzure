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
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        //string Str_Connect = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl98;AccountKey=0Od+makghmoYKNHCBgqUQtlm9t7/0wJQlWZbjkTz8qCJU/QSFITn/TqWTQa/zEkRC33cu0qSWnnv+AStbA4m+Q==;EndpointSuffix=core.windows.net";
        //string Str_Connect2 = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl98;AccountKey=GecQ9fvQOC8fU95LzDE2NRqv7QmXiy+fI4iHMcHE3YVn2KDgwSjxrrxJUjzjMmBNxmoOF38mK+2V+AStB+464w==;EndpointSuffix=core.windows.net";
        string Contenedor, clean, curated, rejected;
        string ExtencionArchivos;
        List<string> NombresArchivos = new List<string>();
        HttpResponseMessage response = new HttpResponseMessage();

        [HttpGet]
        [Route("ExisteClientes")]
        public IActionResult ExisteClientes(string Container, string File, string Ext)
        {
            Azure = new AzureFunctionsClass(Container, Ext);
            DataValidate = Azure.ValidateExistsContainer(File);
            //Str_Connect = Security.Encriptar(Str_Connect);
            if (DataValidate.Columns[0].ColumnName != "ERROR")
            {
                TorF = true;
            }
            try
            {
                if (TorF)
                {
                    response.StatusCode = HttpStatusCode.BadRequest;
                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse = "Contenedor " + Container + " y archivo encontrado " + File + "." + Ext + " encontrados";
                    jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                }
                else
                {
                    response.StatusCode = HttpStatusCode.OK;
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Contenedor " + Container + " y/o archivo " + File + "." + Ext + " no encontrados";
                }
            }
            catch (Exception ex)
            {
                response.StatusCode = HttpStatusCode.BadRequest;
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Ocurrio un error en el proceso: " + ex.Message;
            }

            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("DataCleanVentas")]
        public IActionResult CleanDataVentas(CuratedModel parametros)
        {
            try
            {
                if (parametros.ContenedorSource == null || parametros.ExtencionArchivosN == null || parametros.NombresArchivosN.Count == 0 || parametros.ContenedorCurated == null || parametros.ContenedorRejected == null)
                {
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.ContenedorSource;
                    ExtencionArchivos = parametros.ExtencionArchivosN;
                    NombresArchivos = parametros.NombresArchivosN;
                    curated = parametros.ContenedorCurated;
                    rejected = parametros.ContenedorRejected;
                    Azure = new AzureFunctionsClass(Contenedor, ExtencionArchivos);

                    DataValidate = new DataTable();
                    DataValidate.Columns.Add("Archivo Trabajado");
                    DataValidate.Columns.Add("Archivo Curated");
                    DataValidate.Columns.Add("URL Curated");
                    DataValidate.Columns.Add("Archivo Rejected");
                    DataValidate.Columns.Add("URL Rejected");

                    for (int k = 0; k < NombresArchivos.Count; k++)// este for se deja con un valor en duro, ya que para este ejercicio solo se cuentan con 3 archivos
                    {
                        FileName = NombresArchivos[k];
                        DT_DataSource = new DataTable();
                        DT_DataSource = Azure.TransformFileforAzure(FileName);

                        if (FileName.ToLower().Contains("clientes"))
                        {
                            if (DT_DataSource.Rows.Count > 0)
                            {
                                dataerror = new DataTable();
                                DT_DataSource = Functions.CleanDataTableClientes(DT_DataSource);
                            }
                        }
                        else if (FileName.ToLower().Contains("productos"))
                        {
                            if (DT_DataSource.Rows.Count > 0)
                            {
                                dataerror = new DataTable();
                                DT_DataSource = Functions.CleanDataTableProductos(DT_DataSource);
                            }
                        }
                        else if (FileName.ToLower().Contains("Sucursales"))
                        {
                            if (DT_DataSource.Rows.Count > 0)
                            {
                                dataerror = new DataTable();
                                DT_DataSource = Functions.CleanDataTableSucursales(DT_DataSource);
                            }
                        }
                        else if (FileName.ToLower().Contains("Ventas"))
                        {
                            if (DT_DataSource.Rows.Count > 0)
                            {
                                dataerror = new DataTable();
                                DT_DataSource = Functions.CleanDataTableVentas(DT_DataSource);
                            }
                        }
                        else
                        {
                            DT_DataSource = Functions.CleanDataTable(DT_DataSource);
                        }

                        DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
                        try
                        {
                            dataerror = Functions.GetDTErrores();
                            string limpios, sucios, Upload;
                            string URLlimpios = "";
                            string URLsucios = "";
                            FileName = FileName.Replace("Clean", "");
                            int errorproceso = 0;
                            if (DT_DataSource.Rows.Count > 0)
                            {
                                if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                                {
                                    errorproceso = 1;
                                    Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: ExtencionArchivos + "_Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                    rutaOutput = Azure.GetUrlContainer();
                                    rutaOutput = rutaOutput.Replace(Contenedor, rejected);
                                    URLsucios = rutaOutput + ExtencionArchivos + "_Rejected_" + FileName + ".csv";
                                }
                                else
                                {
                                    Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: ExtencionArchivos + "_Curated_" + FileName + ".csv", table: DT_DataSource, ContainerBlobName: curated);
                                    rutaOutput = Azure.GetUrlContainer();
                                    rutaOutput = rutaOutput.Replace(Contenedor, curated);
                                    URLlimpios = rutaOutput + ExtencionArchivos + "_Curated_" + FileName + ".csv";
                                }
                            }
                            if (dataerror.Rows.Count > 0)
                            {
                                Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: ExtencionArchivos + "_Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                rutaOutput = Azure.GetUrlContainer();
                                rutaOutput = rutaOutput.Replace(Contenedor, rejected);
                                URLsucios = rutaOutput + ExtencionArchivos + "_Rejected_" + FileName + ".csv";
                            }
                            limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
                            if (errorproceso == 1)
                            {
                                sucios = "Filas sucuias:" + DT_DataSource.Rows.Count.ToString() + ". Archivo original dañado";
                            }
                            else
                            {
                                sucios = "Filas sucuias:" + dataerror.Rows.Count.ToString();
                            }
                            DataValidate.Rows.Add(FileName, limpios, URLlimpios, sucios, URLsucios);
                        }
                        catch (Exception ex)
                        {
                            response.StatusCode = HttpStatusCode.BadRequest;
                            jsonresponse.CodeResponse = 0;
                            jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message;
                        }
                    }
                    response.StatusCode = HttpStatusCode.OK;
                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse = "Proceso Terminado con Exito";
                    jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                }
            }
            catch (Exception ex)
            {
                response.StatusCode = HttpStatusCode.BadRequest;
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Error en el proceso CleanData: " + ex.Message;
            }
            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("DataProcessing")]
        public IActionResult DataProcessing(CuratedModel parametros)
        {            
            try
            {
                HttpClient httpClient = new HttpClient();
                httpClient.Timeout = TimeSpan.FromMinutes(10);

                if (parametros.ContenedorSource == null || parametros.ExtencionArchivosN == null || parametros.NombresArchivosN.Count == 0 || parametros.ContenedorCurated == null || parametros.ContenedorRejected == null)
                {
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.ContenedorSource;
                    ExtencionArchivos = parametros.ExtencionArchivosN;
                    NombresArchivos = parametros.NombresArchivosN;
                    curated = parametros.ContenedorCurated;
                    rejected = parametros.ContenedorRejected;
                    Azure = new AzureFunctionsClass(Contenedor, ExtencionArchivos);

                    DataValidate = new DataTable();
                    DataValidate.Columns.Add("Archivo Trabajado");
                    DataValidate.Columns.Add("Archivo Curated");
                    DataValidate.Columns.Add("URL Curated");
                    DataValidate.Columns.Add("Archivo Rejected");
                    DataValidate.Columns.Add("URL Rejected");

                    for (int k = 0; k < NombresArchivos.Count; k++)// este for se deja con un valor en duro, ya que para este ejercicio solo se cuentan con 3 archivos
                    {
                        FileName = NombresArchivos[k];
                        DT_DataSource = new DataTable();
                        DT_DataSource = Azure.TransformFileforAzure(FileName);
                        DT_DataSource=Functions.DropDuplicates(DT_DataSource);
                        DT_DataSource = Functions.CleanDataTable(DT_DataSource);
                        DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
                        try
                        {
                            dataerror = Functions.GetDTErrores();
                            string limpios, sucios, Upload;
                            string URLlimpios = "";
                            string URLsucios = "";                            
                            FileName = FileName.Replace("Clean", "");
                            int errorproceso = 0;
                            if (DT_DataSource.Rows.Count > 0)
                            {                                
                                if (DT_DataSource.Columns[0].ColumnName.ToLower().Contains("error"))
                                {
                                    errorproceso = 1;
                                    Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: ExtencionArchivos + "_Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                    rutaOutput = Azure.GetUrlContainer();
                                    rutaOutput = rutaOutput.Replace(Contenedor, rejected);
                                    URLsucios = rutaOutput + ExtencionArchivos + "_Rejected_" + FileName + ".csv";
                                }
                                else
                                {
                                    Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: ExtencionArchivos + "_Curated_" + FileName + ".csv", table: DT_DataSource, ContainerBlobName: curated);
                                    rutaOutput = Azure.GetUrlContainer();
                                    rutaOutput = rutaOutput.Replace(Contenedor, curated);
                                    URLlimpios = rutaOutput + ExtencionArchivos + "_Curated_" + FileName + ".csv";
                                }
                            }
                            if (dataerror.Rows.Count > 0)
                            {
                                Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: ExtencionArchivos + "_Rejected_" + FileName + ".csv", table: dataerror, ContainerBlobName: rejected);
                                rutaOutput = Azure.GetUrlContainer();
                                rutaOutput = rutaOutput.Replace(Contenedor, rejected);
                                URLsucios = rutaOutput + ExtencionArchivos + "_Rejected_" + FileName + ".csv";
                            }
                            limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
                            if (errorproceso == 1)
                            {
                                sucios = "Filas sucuias:" + DT_DataSource.Rows.Count.ToString() + ". Archivo original dañado";
                            }
                            else
                            {
                                sucios = "Filas sucuias:" + dataerror.Rows.Count.ToString();
                            }
                            DataValidate.Rows.Add(FileName, limpios, URLlimpios, sucios, URLsucios);
                        }
                        catch (Exception ex)
                        {
                            response.StatusCode = HttpStatusCode.BadRequest;
                            jsonresponse.CodeResponse = 0;
                            jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message;
                        }
                    }                    
                    response.StatusCode = HttpStatusCode.OK;
                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse = "Proceso Terminado con Exito";
                    jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                }
            }
            catch (Exception ex)
            {
                response.StatusCode = HttpStatusCode.BadRequest;
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Error en el proceso CleanData: " + ex.Message;
            }
            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("DataClean")]
        public IActionResult CleanData(CleanModel parametros) //from raw to clean
        {
            try
            {
                if (parametros.ContenedorRAW == null || parametros.ContenedorClean == null || parametros.ExtencionArchivosN == null || parametros.NombresArchivosN.Count == 0)
                {
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Parametros vacios";
                }
                else
                {
                    Contenedor = parametros.ContenedorRAW;
                    clean=parametros.ContenedorClean;
                    ExtencionArchivos = parametros.ExtencionArchivosN;
                    NombresArchivos = parametros.NombresArchivosN;
                    Azure = new AzureFunctionsClass(Contenedor, ExtencionArchivos);

                    DataValidate = new DataTable();
                    DataValidate.Columns.Add("Archivo Trabajado");
                    DataValidate.Columns.Add("URL Archivo");

                    for (int k = 0; k < NombresArchivos.Count; k++)// este for se deja con un valor en duro, ya que para este ejercicio solo se cuentan con 3 archivos
                    {
                        DT_DataSource = new DataTable();
                        dataerror = new DataTable();

                        FileName = NombresArchivos[k];
                        DT_DataSource = Azure.TransformFileforAzure(FileName);
                        DT_DataSource = Functions.DropDuplicates(DT_DataSource);
                        try
                        {
                            dataerror = Functions.GetDTErrores();
                            string limpios, Upload;
                            string URL = "";
                            //FileName = FileName.Replace("Clean", "");
                            if (DT_DataSource.Rows.Count > 0)
                            {
                                Upload = Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: FileName + "." + ExtencionArchivos, table: DT_DataSource, ContainerBlobName: clean);
                                rutaOutput = Azure.GetUrlContainer();
                                URL = rutaOutput + FileName + "." + ExtencionArchivos;
                            }
                            limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
                            DataValidate.Rows.Add(FileName, URL);
                        }
                        catch (Exception ex)
                        {
                            response.StatusCode = HttpStatusCode.BadRequest;
                            jsonresponse.CodeResponse = 0;
                            jsonresponse.MessageResponse = "Error al enviar archivos al contenedor " + Contenedor + " y el archivo " + NombresArchivos[k].ToString() + ": " + ex.Message;
                        }
                    }
                    response.StatusCode = HttpStatusCode.OK;
                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse = "Proceso Terminado con Exito";
                    jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                }
            }
            catch (Exception ex)
            {
                response.StatusCode = HttpStatusCode.BadRequest;
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Error en el proceso CleanData: " + ex.Message;
            }
            return Json(jsonresponse);
        }
    }
}

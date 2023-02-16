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

        DataTable DataValidate = new DataTable();
        Boolean TorF = false;
        string rutaOutput = "";
        string FileName = "";
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        string Str_Connect = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl98;AccountKey=0Od+makghmoYKNHCBgqUQtlm9t7/0wJQlWZbjkTz8qCJU/QSFITn/TqWTQa/zEkRC33cu0qSWnnv+AStbA4m+Q==;EndpointSuffix=core.windows.net";

        [HttpGet]
        [Route("ExisteClientes")]
        public IActionResult ExisteClientes(string Container, string File, string Ext)
        {
            Azure = new AzureFunctionsClass(Container, Ext);
            DataValidate = Azure.ValidateExistsContainer(File);

            if (DataValidate.Columns[0].ColumnName != "ERROR")
            {
                TorF = true;
            }
            try
            {
                if (TorF)
                {
                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse = "Contenedor " + Container + " y archivo encontrado " + File + Ext + " encontrados";
                    jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                }
                else
                {
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "Contenedor " + Container + " y/o archivo encontrado " + File + Ext + " no encontrados";
                }
            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Ocurrio un error en el proceso: " + ex.Message;
            }

            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("DataClean")]
        public IActionResult CleanData(string Contenedor, string ExtencionArchivos, List<string> NombresArchivos)
        {            
            try
            {
                Azure = new AzureFunctionsClass(Contenedor, ExtencionArchivos);
                DataValidate = new DataTable();
                DataValidate.Columns.Add("Archivo Trabajado");
                DataValidate.Columns.Add("Archivo Curated");
                DataValidate.Columns.Add("Archivo Rejected");

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
                            Functions.CopyHeaders(DT_DataSource, dataerror);
                            DT_DataSource = Functions.CleanDataTableClientes(DT_DataSource);
                        }

                    }
                    else if (FileName.ToLower().Contains("productos"))
                    {
                        if (DT_DataSource.Rows.Count > 0)
                        {
                            dataerror = new DataTable();
                            Functions.CopyHeaders(DT_DataSource, dataerror);
                            DT_DataSource = Functions.CleanDataTableProductos(DT_DataSource);
                        }

                    }
                    else if (FileName.ToLower().Contains("Sucursales"))
                    {
                        if (DT_DataSource.Rows.Count > 0)
                        {
                            dataerror = new DataTable();
                            Functions.CopyHeaders(DT_DataSource, dataerror);
                            DT_DataSource = Functions.CleanDataTableSucursales(DT_DataSource);
                        }

                    }
                    else if (FileName.ToLower().Contains("Ventas"))
                    {
                        if (DT_DataSource.Rows.Count > 0)
                        {
                            dataerror = new DataTable();
                            Functions.CopyHeaders(DT_DataSource, dataerror);
                            DT_DataSource = Functions.CleanDataTableVentas(DT_DataSource);
                        }

                    }

                    DT_DataSource = Functions.DeleteDirtyRows(DT_DataSource);
                    try
                    {
                        string limpios, sucios;
                        rutaOutput = Azure.GetUrlContainer();
                        FileName = FileName.Replace("Clean", "");                        
                        if (DT_DataSource.Rows.Count > 0)
                        {
                            Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "Curated_" + FileName, table: DT_DataSource);                            
                        }                       
                        if (dataerror.Rows.Count > 0)
                        {
                            Azure.UploadBlobDLSG2(PathBlob: rutaOutput, FilenameAz: "Rejected_" + FileName, table: dataerror);                            
                        }
                        limpios = "Filas limpias:" + DT_DataSource.Rows.Count.ToString();
                        sucios = "Filas sucuias:" + dataerror.Rows.Count.ToString();
                        DataValidate.Rows.Add(FileName, limpios, sucios);                        
                    }
                    catch (Exception ex)
                    {
                        jsonresponse.CodeResponse = 0;
                        jsonresponse.MessageResponse = "Error al enviar archivos al contenedor "+ Contenedor + " y el archivo "+ NombresArchivos[k].ToString()+": " + ex.Message;
                    }
                }
                jsonresponse.CodeResponse = 1;
                jsonresponse.MessageResponse = "Proceso Terminado con Exito";
                jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Error en el proceso CleanData: " + ex.Message;
            }

            return View();
        }
    }
}

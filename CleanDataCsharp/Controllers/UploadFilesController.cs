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

namespace CleanDataCsharp.Controllers
{
    [ApiController]
    [Route("Azure")]
    public class UploadFilesController : Controller
    {
        FunctionsClass Functions=new FunctionsClass();
        ResponsesModel jsonresponse = new ResponsesModel();

        DataTable DataValidate=new DataTable();
        Boolean TorF=false;
        string Str_Connect = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl98;AccountKey=0Od+makghmoYKNHCBgqUQtlm9t7/0wJQlWZbjkTz8qCJU/QSFITn/TqWTQa/zEkRC33cu0qSWnnv+AStbA4m+Q==;EndpointSuffix=core.windows.net";

        [HttpGet]
        [Route("ExisteClientes")]
        public dynamic ExisteClientes(string Container)
        {
            DataValidate.Columns.Add("Contenedores");
            DataValidate.Rows.Add("datacontainer");
            
            if (DataValidate.Rows.Count > 0)
            {
                TorF = true;
            }
            try
            {
                if (TorF)
                {
                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse = "Contenedor "+ Container + " encontrado";
                    jsonresponse.ListResponse = Functions.ConvertDataTableToDicntionary(DataValidate);
                }
                else
                {
                    jsonresponse.CodeResponse = 0;
                    jsonresponse.MessageResponse = "No se encontro el contenedor";
                }
            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Ocurrio un error en el proceso: "+ex.Message;
            }

            return Json(jsonresponse);
        }

        [HttpPost]
        [Route("Clientes")]
        public IActionResult Index()
        {
            return View();
        }
    }
}

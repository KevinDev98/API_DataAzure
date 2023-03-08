using Azure.Storage.Blobs;
using System.ComponentModel;
using System.Data;
using System.Net;
using System;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
using System.Runtime.CompilerServices;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage;
using CleanDataCsharp.Models;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Hosting.Server;
using System.Xml;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
//using Microsoft.Azure.Storage;

namespace CleanDataCsharp.Class
{
    public class AzureFunctionsClass
    {
        static string ContainerNameA = "";
        public static string ExtenFile = "";
        //static string FileName;
        FunctionsClass Functions = new FunctionsClass();
        ResponsesModel jsonresponse = new ResponsesModel();
        SecurityClass Security = new SecurityClass();

        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        DataTable DataValidate = new DataTable();
        WebClient clientWeb;
        Stream streamAzure;
        StreamReader readerFileAzure;
        public IConfiguration _Configuration;
        string Str_Connect;        
        //public AzureFunctionsClass(IConfiguration configuration)
        //{
        //    _Configuration = configuration;
        //}
        public AzureFunctionsClass(string ContainerName)//, string FileExtension)
        {
            _Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
            var Az = _Configuration.GetSection("AzureConf").Get<AzureCon>();
            ContainerNameA = ContainerName;
            Str_Connect =Az.keyblob;
            //ExtenFile = FileExtension;
        }
        #region Var Azure
        static string Str_Connect2 = "";
        string rutaDLSG2_Clean;
        static BlobContainerClient container;
        static BlobClient BlobStrg;
        #endregion                   

        #region UploadBlobAzure
        public void UploadFileDLSG2(FileInfo UploadFile) //METODO 1 PARA CARGAR DATOS A UN CONTENEDOR
        {
            try
            {
                container.DeleteBlobIfExists(UploadFile.Name);
                container.UploadBlob(UploadFile.Name, UploadFile.OpenRead());
                //Console.ForegroundColor = ConsoleColor.Green;
                //Console.WriteLine("Archico cargado correctamente" + UploadFile.Name);
            }
            catch (Exception ex)
            {
                //Console.ForegroundColor = ConsoleColor.Red;
                //Console.WriteLine("Azure error: " + ex.Message);
            }
        }

        public string GetUrlContainer()//Obtiene la URL del contenedor
        {
            Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
            BlobStrg = new BlobClient(Str_Connect2, ContainerNameA, "");
            string url = BlobStrg.Uri.ToString() + "/";
            //url = url.Replace(TableName + ".csv", "");
            return url;
        }
        public DataTable ValidateExistsContainer(string FileName)
        {
            try
            {
                Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
                BlobStrg = new BlobClient(Str_Connect, ContainerNameA, FileName + "." + ExtenFile);
                clientWeb = new WebClient();
                FileName = BlobStrg.Name;

                rutaDLSG2_Clean = BlobStrg.Uri.ToString();
                streamAzure = clientWeb.OpenRead(rutaDLSG2_Clean);//Intenta leer el archivo, si no existe saltará al catch
                DataValidate.Columns.Add("Contenedores");
                DataValidate.Rows.Add(FileName);
            }
            catch (Exception ex)
            {
                DataValidate = new DataTable();
                DataValidate.Columns.Add("ERROR VALIDATE");
                DataValidate.Rows.Add("ERROR: " + ex.Message + " Validar que el contenedor especificado tenga los permisos de acceso necesarios");
            }

            return DataValidate;
        }
        public DataTable TransformFileforAzure(string FileName, string delimiter="")
        {
            try
            {
                Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
                BlobStrg = new BlobClient(Str_Connect2, ContainerNameA, FileName);
                clientWeb = new WebClient();

                rutaDLSG2_Clean = BlobStrg.Uri.ToString();
                FileName = BlobStrg.Name;
                streamAzure = clientWeb.OpenRead(rutaDLSG2_Clean);//Transforma los datos del archivo de origen
                readerFileAzure = new StreamReader(streamAzure);//Transforma la data en archivo
                if (FileName.ToLower().Contains(".csv") || FileName.ToLower().Contains(".txt"))
                {
                    if (string.IsNullOrEmpty(delimiter))
                    {
                        delimiter= ",";
                    }
                    DT_DataSource = Functions.FromCsvForDataTable(readerFileAzure,delimiter);//Manda a transformar el archivo a DataTable
                }
                else if (FileName.ToLower().Contains(".xml"))
                {
                    DT_DataSource = Functions.FromXlmToDataTable(readerFileAzure);
                }
                else if (FileName.ToLower().Contains(".json"))
                {
                    DT_DataSource = Functions.FromJsonToDataTable(readerFileAzure);
                }
            }
            catch (Exception ex)
            {
                DT_DataSource = new DataTable();
                DT_DataSource.Columns.Add("ERROR TRANSFORM");
                DT_DataSource.Rows.Add("ERROR: " + ex.Message + " Validar que el contenedor especificado tenga los permisos de acceso necesarios");
            }

            return DT_DataSource;
        }
        public List<String> ListFile(string PathBlob, string ContainerBlobName)
        {
            List<String> listName = new List<String>();
            Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
            BlobContainerClient containerClient = new BlobContainerClient(Str_Connect2, ContainerNameA);//Recibe cadena de conexion y nombre de contenedor
            var ListblobFiles = containerClient.GetBlobs();
            foreach (BlobItem blobItem in ListblobFiles)
            {
                listName.Add(blobItem.Name);
            }
            //container.UploadBlob();
            return listName;
        }
        public string UploadBlobDLSG2(string FilenameAz, DataTable table, string ContainerBlobName) //Carga el archivo a DLS 
        {
            //Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Str_Connect2);//Se inicia conexión
            //CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient(); //Se instancia un blob Client
            //CloudBlobContainer containercloud = blobClient.GetContainerReference(ContainerNameA);//NOMBRE DEL CONTENEDOR AL QUE SE HACE REF
            //string filePathRoot = FilenameAz;//URL DEL CONTENEDOR
            //CloudBlobDirectory cloudBlobDirectory = containercloud.GetDirectoryReference(filePathRoot);
            //var blockBlob = containercloud.GetBlockBlobReference(FilenameAz);
            try
            {
                Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
                BlobServiceClient AzureBlobStorage = new BlobServiceClient(Str_Connect2);
                container = AzureBlobStorage.GetBlobContainerClient(ContainerBlobName);
                container.DeleteBlobIfExists(FilenameAz); //Borra el archivo si ya existe
                byte[] blobBytes = null;
                blobBytes = Functions.FromTableToCSV(table);
                using (var readStream = new MemoryStream(blobBytes))
                {
                    container.UploadBlob(FilenameAz, readStream);//Carga el archivo                        
                }
                jsonresponse.CodeResponse = 1;
                jsonresponse.MessageResponse = "Proceso Correcto";
            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Azure blob error: " + ex.Message+ "_"+ ex.InnerException;
            }
            return jsonresponse.MessageResponse;
        }
        public string RemoveFiles(string FilenameAz, string ContainerBlobName)
        {
            //Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
            //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Str_Connect2);//Se inicia conexión
            //CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient(); //Se instancia un blob Client
            //CloudBlobContainer containercloud = blobClient.GetContainerReference(ContainerNameA);//NOMBRE DEL CONTENEDOR AL QUE SE HACE REF
            //string filePathRoot = FilenameAz;//URL DEL CONTENEDOR
            //CloudBlobDirectory cloudBlobDirectory = containercloud.GetDirectoryReference(filePathRoot);
            //var blockBlob = containercloud.GetBlockBlobReference(FilenameAz);
            try
            {
                Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
                BlobServiceClient AzureBlobStorage = new BlobServiceClient(Str_Connect2);
                container = AzureBlobStorage.GetBlobContainerClient(ContainerBlobName);
                container.DeleteBlobIfExists(FilenameAz); //Borra el archivo si ya existe                
                jsonresponse.CodeResponse = 1;
                jsonresponse.MessageResponse = "Proceso Correcto";
            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Azure blob error: " + ex.Message+ "_"+ ex.InnerException;
                //Console.ForegroundColor = ConsoleColor.Red;
                //Console.WriteLine("Azure blob error: " + ex.Message);
            }
            return jsonresponse.MessageResponse;
        }
        public string FromSQLtoBlobDLSG2(string FilenameAz, DataTable table) //Carga el archivo a DLS 
        {
            string result;
            try
            {
                Str_Connect2 = Str_Connect; //Security.DesEncriptar(Str_Connect);
                BlobServiceClient AzureBlobStorage = new BlobServiceClient(Str_Connect2);
                container = AzureBlobStorage.GetBlobContainerClient(ContainerNameA);
                container.DeleteBlobIfExists(FilenameAz); //Borra el archivo si ya existe

                byte[] blobBytes= Functions.FromTableToCSV(table);                
                using (var readStream = new MemoryStream(blobBytes))
                {
                    container.UploadBlob(FilenameAz, readStream);//Carga el archivo
                }
                result = "success";
            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Azure blob error: " + ex.Message + "_" + ex.InnerException;
                result = "fail "+ "Azure blob error: " + ex.Message + "_" + ex.InnerException; ;
            }
            return result;
        }
        #endregion
    }
}

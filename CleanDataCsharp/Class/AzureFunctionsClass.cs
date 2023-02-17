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

namespace CleanDataCsharp.Class
{
    public class AzureFunctionsClass
    {
        static string ContainerNameA = "";
        public static string ExtenFile = "";
        static string FileName;
        FunctionsClass Functions = new FunctionsClass();
        ResponsesModel jsonresponse = new ResponsesModel();
        SecurityClass Security = new SecurityClass();

        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        DataTable DataValidate = new DataTable();
        WebClient clientWeb;
        Stream streamAzure;
        StreamReader readerFileAzure;
        public AzureFunctionsClass(string ContainerName, string FileExtension)
        {
            ContainerNameA = ContainerName;
            ExtenFile = FileExtension;
        }
        #region Var Azure
        static string Str_Connect = "RABlAGYAYQB1AGwAdABFAG4AZABwAG8AaQBuAHQAcwBQAHIAbwB0AG8AYwBvAGwAPQBoAHQAdABwAHMAOwBBAGMAYwBvAHUAbgB0AE4AYQBtAGUAPQBzAHQAbwByAGEAZwBlAGEAYwBjAG8AdQBuAHQAZQB0AGwAOQA4ADsAQQBjAGMAbwB1AG4AdABLAGUAeQA9ADAATwBkACsAbQBhAGsAZwBoAG0AbwBZAEsATgBIAEMAQgBnAHEAVQBRAHQAbABtADkAdAA3AC8AMAB3AEoAUQBsAFcAWgBiAGoAawBUAHoAOABxAEMASgBVAC8AUQBTAEYASQBUAG4ALwBUAHEAVwBUAFEAYQAvAHoARQBrAFIAQwAzADMAYwB1ADAAcQBTAFcAbgBuAHYAKwBBAFMAdABiAEEANABtACsAUQA9AD0AOwBFAG4AZABwAG8AaQBuAHQAUwB1AGYAZgBpAHgAPQBjAG8AcgBlAC4AdwBpAG4AZABvAHcAcwAuAG4AZQB0AA==";
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
        public DataTable FromXlmToDataTable(string xml)
        {
            DataSet ds = new DataSet();
            ds.ReadXml(new XmlTextReader(new StringReader(xml)));

            DataTable data = ds.Tables[0];
            return data;
        }
        public DataTable FromJsonToDataTable(string jsonData)
        {
            DataTable data = JsonConvert.DeserializeObject<DataTable>(jsonData);
            return data;
        }
        public DataTable FromCsvForDataTable(StreamReader DataReaderCSV) //Recibe un CSV de Azure y lo transforma en DataTable
        {
            DataTable dt = new DataTable();
            using (DataReaderCSV)
            {
                string[] headers = DataReaderCSV.ReadLine().Split(",");
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                try
                {
                    while (!DataReaderCSV.EndOfStream)
                    {
                        string[] rows = DataReaderCSV.ReadLine().Split(",");
                        DataRow dr = dt.NewRow();
                        string data;
                        for (int i = 0; i < headers.Length; i++)
                        {
                            data = rows[i];
                            if (rows[i].Contains(":") & rows[i].Contains("/")) //Elimina los espacios en blanco de las columnas que no son hora
                            {
                                data = data.Replace(" 00:00", "").Replace(" 00:00:00.0000000", ""); //Elimina caracteres inecesarios
                                rows[i] = data;
                            }
                            rows[i] = Functions.Remove_SpacheWithe(Functions.Remove_Special_Characteres(data));
                            dr[i] = rows[i];
                        }
                        dt.Rows.Add(dr);
                    }
                }
                catch (Exception EX)
                {
                    //Console.WriteLine(EX.Message);
                }
            }
            return dt;
        } //Lee un archivo del contenedor y lo transforma en DataTable para poder limpiar la data        
        public string GetUrlContainer()//Obtiene la URL del contenedor
        {
            Str_Connect2 = Security.DesEncriptar(Str_Connect);
            BlobStrg = new BlobClient(Str_Connect2, ContainerNameA, "");
            string url = BlobStrg.Uri.ToString() + "/";
            //url = url.Replace(TableName + ".csv", "");
            return url;
        }
        public DataTable ValidateExistsContainer(string FileName)
        {
            try
            {
                Str_Connect2 = Security.DesEncriptar(Str_Connect);
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
                DataValidate=new DataTable();
                DataValidate.Columns.Add("ERROR VALIDATE");
                DataValidate.Rows.Add("ERROR: " + ex.Message+" Validar que el contenedor especificado tenga los permisos de acceso necesarios");
            }           

            return DataValidate;
        }
        public DataTable TransformFileforAzure(string FileName)
        {
            try
            {
                Str_Connect2 = Security.DesEncriptar(Str_Connect);
                BlobStrg = new BlobClient(Str_Connect2, ContainerNameA, FileName + "." + ExtenFile.ToLower());
                clientWeb = new WebClient();

                rutaDLSG2_Clean = BlobStrg.Uri.ToString();
                FileName = BlobStrg.Name;                
                streamAzure = clientWeb.OpenRead(rutaDLSG2_Clean);//Transforma los datos del archivo de origen
                readerFileAzure = new StreamReader(streamAzure);//Transforma la data en archivo
                if (ExtenFile.ToLower() == "csv")
                {
                    DT_DataSource = FromCsvForDataTable(readerFileAzure);//Manda a transformar el archivo a DataTable
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

        public string UploadBlobDLSG2(string PathBlob, string FilenameAz, DataTable table, string ContainerBlobName) //Carga el archivo a DLS 
        {
            //?restype=container&comp=list
            Str_Connect2 = Security.DesEncriptar(Str_Connect);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Str_Connect2);//Se inicia conexión
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient(); //Se instancia un blob Client
            CloudBlobContainer containercloud = blobClient.GetContainerReference(ContainerNameA);//NOMBRE DEL CONTENEDOR AL QUE SE HACE REF
            string filePathRoot = FilenameAz;//URL DEL CONTENEDOR
            CloudBlobDirectory cloudBlobDirectory = containercloud.GetDirectoryReference(filePathRoot);
            var blockBlob = containercloud.GetBlockBlobReference(FilenameAz);

            try
            {
                byte[] blobBytes;
                using (var writeStream = new MemoryStream()) //Transforma el Stream a archivos
                {
                    using (var writer = new StreamWriter(writeStream))
                    {
                        //table.WriteXml(writer, XmlWriteMode.WriteSchema);
                        for (int i = 0; i < table.Columns.Count; i++)
                        {
                            writer.Write(table.Columns[i]);
                            if (i < table.Columns.Count - 1)
                            {
                                writer.Write(",");
                            }
                        }
                        writer.Write(writer.NewLine);
                        foreach (DataRow dr in table.Rows)
                        {
                            for (int i = 0; i < table.Columns.Count; i++)
                            {
                                if (!Convert.IsDBNull(dr[i]))
                                {
                                    string value = dr[i].ToString();
                                    if (value.Contains(','))
                                    {
                                        value = String.Format("\"{0}\"", value);
                                        writer.Write(value);
                                    }
                                    else
                                    {
                                        writer.Write(dr[i].ToString());
                                    }
                                }
                                if (i < table.Columns.Count - 1)
                                {
                                    writer.Write(",");
                                }
                            }
                            writer.Write(writer.NewLine);
                        }
                        writer.Close();
                    }
                    blobBytes = writeStream.ToArray();
                }
                using (var readStream = new MemoryStream(blobBytes))
                {
                    Str_Connect2 = Security.DesEncriptar(Str_Connect);
                    BlobServiceClient AzureBlobStorage = new BlobServiceClient(Str_Connect2);

                    container = AzureBlobStorage.GetBlobContainerClient(ContainerBlobName);
                    container.DeleteBlobIfExists(FilenameAz); //Borra el archivo si ya existe
                    container.UploadBlob(FilenameAz, readStream);//Carga el archivo

                    jsonresponse.CodeResponse = 1;
                    jsonresponse.MessageResponse="Proceso Correcto";
                }

            }
            catch (Exception ex)
            {
                jsonresponse.CodeResponse = 0;
                jsonresponse.MessageResponse = "Azure blob error: " + ex.Message;
                //Console.ForegroundColor = ConsoleColor.Red;
                //Console.WriteLine("Azure blob error: " + ex.Message);
            }
            return jsonresponse.MessageResponse;
        }
        #endregion
    }
}

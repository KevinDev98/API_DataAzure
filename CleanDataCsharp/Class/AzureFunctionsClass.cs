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

namespace CleanDataCsharp.Class
{
    public class AzureFunctionsClass
    {
        FunctionsClass Functions = new FunctionsClass();
        #region Var Azure
        static string Str_Connect = "DefaultEndpointsProtocol=https;AccountName=storageaccountetl3;AccountKey=2Xe/Yjm77kNncRixR9B6LD+2rMMsvQIMSyJaxjHv8tGbH3tuKwanLLoy/IPNrEbzyvZ6J3wIi9I4+AStkiYVpQ==;EndpointSuffix=core.windows.net";
        static string ContainerNameA = "containercleaned";
        string rutaDLSG2_Clean;

        static BlobServiceClient AzureBlobStorage = new BlobServiceClient(Str_Connect);
        static BlobContainerClient container = AzureBlobStorage.GetBlobContainerClient(ContainerNameA);
        static BlobClient BlobStrg;
        #endregion     
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        #region UploadBlobAzure
        public void UploadFileDLSG2(FileInfo UploadFile) //METODO 1 PARA CARGAR DATOS A UN CONTENEDOR
        {
            try
            {
                container.DeleteBlobIfExists(UploadFile.Name);
                container.UploadBlob(UploadFile.Name, UploadFile.OpenRead());
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Archico cargado correctamente" + UploadFile.Name);
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Azure error: " + ex.Message);
            }
        }
        public DataTable ReadFileFromAzure(StreamReader DataReaderCSV)
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
                    Console.WriteLine(EX.Message);
                }
            }
            return dt;
        } //Lee un archivo del contenedor y lo transforma en DataTable para poder limpiar la data
        string FileName = "";
        public string GetUrlContainer()
        {
            BlobStrg = new BlobClient(Str_Connect, ContainerNameA, "");
            string url = BlobStrg.Uri.ToString() + "/";
            //url = url.Replace(TableName + ".csv", "");
            return url;
        }
        DataTable CSVtoTableForAzure(string TableName)
        {
            BlobStrg = new BlobClient(Str_Connect, ContainerNameA, "Clean" + TableName + ".csv");
            rutaDLSG2_Clean = BlobStrg.Uri.ToString();
            FileName = BlobStrg.Name;

            WebClient clientWeb;
            Stream streamAzure;
            StreamReader readerFileAzure;

            clientWeb = new WebClient();
            streamAzure = clientWeb.OpenRead(rutaDLSG2_Clean);//Transforma los datos del archivo de origen
            readerFileAzure = new StreamReader(streamAzure);//Transforma la data en archivo

            DT_DataSource = ReadFileFromAzure(readerFileAzure);//Manda a transformar el archivo a DataTable
            return DT_DataSource;
        }

        void UploadBlobDLSG2(string PathBlob, string FilenameAz, DataTable table) //Carga el archivo a DLS 
        {

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Str_Connect);//Se inicia conexión
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient(); //Se instancia un blob Client
            CloudBlobContainer containercloud = blobClient.GetContainerReference(ContainerNameA);//NOMBRE DEL CONTENEDOR AL QUE SE HACE REF
            string filePathRoot = FilenameAz;//URL DEL CONTENEDOR
            CloudBlobDirectory cloudBlobDirectory = containercloud.GetDirectoryReference(filePathRoot);
            var blockBlob = containercloud.GetBlockBlobReference(FilenameAz);

            try
            {
                byte[] blobBytes;
                using (var writeStream = new MemoryStream())
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
                    container.DeleteBlobIfExists(FilenameAz); //Borra el archivo si ya existe
                    container.UploadBlob(FilenameAz, readStream);//Carga el archivo
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("Archivo cargado a contenedor correctamente".ToUpper());
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Azure blob error: " + ex.Message);
            }
        }
        #endregion
    }
}

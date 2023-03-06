using Google.Apis.Services;
using Google.Apis.Discovery;
using Google.Apis.Download;
using Google.Apis.Drive;
using System;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2.Flows;
using Google.Apis.Auth.OAuth2.Responses;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Util.Store;
using Microsoft.VisualBasic;
using Google.Apis.Drive.v3.Data;
using File = System.IO.File;
//using File = Google.Apis.Drive.v3.Data.File;

namespace CleanDataCsharp.Class
{
    public class GoogleDClass
    {
        private DriveService Service = new DriveService();
        IEnumerable<string> scope = new List<string>() { DriveService.Scope.Drive };
        private void CreateService()
        {
            var ClientId = "413144081358-jil4gidoreefqopbpqjefg32eaq9tq94.apps.googleusercontent.com";
            var ClientSecret = "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";
            UserCredential DriveCredential = GoogleWebAuthorizationBroker.AuthorizeAsync(new ClientSecrets()
            {
                ClientId = ClientId,
                ClientSecret = ClientSecret
            },
            scopes: scope,
            user: "", taskCancellationToken: CancellationToken.None).Result;
            Service = new DriveService(new BaseClientService.Initializer() { HttpClientInitializer = DriveCredential, ApplicationName = "ReadFiles NET", ApiKey = "AIzaSyCW5zcojo9X5WErlWXSCUlVG5je9icpwnI" });
            //Service.ApiKey = "AIzaSyCW5zcojo9X5WErlWXSCUlVG5je9icpwnI";
        }
        public void authdrive()
        {
            UserCredential credential;
            using (var stream = new FileStream("credentialNET.json", FileMode.Open, FileAccess.Read))
            {
                string tokenpath = "token.json";
                credential = GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    scope,
                    "kevdan.devti@gmail.com",
                    CancellationToken.None,
                    new FileDataStore(tokenpath, true)).Result;
            }
            Service = new DriveService(new BaseClientService.Initializer() { HttpClientInitializer = credential, ApplicationName = "ReadFiles NET" });
        }
        public void UploadFile()
        {
            // Not needed from a Console app:
            // Me.Cursor = Cursors.WaitCursor
            if (Service.ApplicationName != "ReadFiles NET")
            {
                authdrive();
            }
            //FilesResource.CreateRequest UploadRequest;
            //byte[] ByteArray = System.IO.File.ReadAllBytes(FilePath);
            //System.IO.MemoryStream Stream = new System.IO.MemoryStream(ByteArray);
            //FilesResource.InsertMediaUpload UploadRequest = Service.Files.Insert(TheFile, Stream, TheFile.MimeType);
            FilesResource.GetRequest exportRequest = Service.Files.Get("11jM_hinRavhNhPqFB2QMubRE_y4qJuwC");
        }
    }
}

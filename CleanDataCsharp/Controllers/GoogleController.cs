using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Google.Apis.Discovery;
using Google.Apis.Download;
using Google.Apis.Drive;
using CleanDataCsharp.Models;
using CleanDataCsharp.Class;

namespace CleanDataCsharp.Controllers
{
    [Route("Google")]
    [ApiController]
    public class GoogleController : ControllerBase
    {        
        [HttpPost]
        [Route("GetFileDrive")]
        public dynamic GetFileDrive()
        {
            GoogleDClass GD = new GoogleDClass();
            GD.UploadFile();
            return new {
                success = true,
                message = "token generado correctamente",
                result = "ok"
            };
        }
    }
}

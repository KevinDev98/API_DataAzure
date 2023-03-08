using System.Net;

namespace CleanDataCsharp.Models
{
    public class ResponsesModel
    {
        public int CodeResponse { get; set; }
        public string? MessageResponse { get; set; }
        public List<Dictionary<string, object>>? ListResponse { get; set; }
        public HttpStatusCode status { get; set; } 
        public string? solicitante { get; set; }
    }
}

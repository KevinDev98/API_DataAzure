namespace CleanDataCsharp.Models
{
    public class ResponsesModel
    {
        public int CodeResponse { get; set; }
        public HttpResponseMessage? Response { get; set; }
        public string MessageResponse { get; set; }
        public List<Dictionary<string, object>> ListResponse { get; set; }
    }
}

namespace CleanDataCsharp.Models
{
    public class TransformedModel
    {
        public string? ContenedorSource { get; set; }
        public string? ContenedorTransformed { get; set; }
        public string? ContenedorRejected { get; set; }
        //public string? ExtencionArchivosN { get; set; }
        public List<string>? NombresArchivosN { get; set; }
        public string? delimitador { get; set; } 
        public string? usuarioemail { get; set; }
    }
}

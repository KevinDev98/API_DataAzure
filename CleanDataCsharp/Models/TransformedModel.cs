namespace CleanDataCsharp.Models
{
    public class TransformedModel
    {
        public string? ContenedorOrigen { get; set; }
        public string? ContenedorTransformed { get; set; }
        public string? ContenedorRejected { get; set; }
        //public string? ExtencionArchivosN { get; set; }
        public List<string>? NombresArchivosN { get; set; }
        public string? usuarioemail { get; set; }
    }
}

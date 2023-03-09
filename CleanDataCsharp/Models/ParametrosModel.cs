namespace CleanDataCsharp.Models
{
    public class ParametrosModel
    {
        public string? ContenedorOrigen { get; set; }
        public string? ContenedorDestino { get; set; }
        //public string? ExtencionArchivosN { get; set; }
        public List<string> NombresArchivos { get; set; }
        public string? delimitador { get; set; }
        public string? MergeFileName { get; set; }
        public string? usuarioemail { get; set; }
    }
}

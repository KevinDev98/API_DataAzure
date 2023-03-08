namespace CleanDataCsharp.Models
{
    public class RawModel
    {
        public string ContenedorIngesta { get; set; }
        public string ContenedorRAW { get; set; }
        //public string ExtencionArchivosOrigen { get; set; }
        public List<string> NombresArchivosN { get; set; }
        public string? delimitador { get; set; }
        public string? usuarioemail { get; set; }
    }
}

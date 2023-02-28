namespace CleanDataCsharp.Models
{
    public class ParametrosModel
    {
        public string? ContenedorSource { get; set; }
        public string? ContenedorCurated { get; set; }
        public string? ContenedorRejected { get; set; }
        //public string? ExtencionArchivosN { get; set; }
        public List<string> NombresArchivosN { get; set; }
        public string key { get; set; }
    }
}

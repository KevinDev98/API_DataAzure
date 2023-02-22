namespace CleanDataCsharp.Models
{
    public class CleanModel
    {
        public string Contenedor { get; set; }
        public string? ContenedorRAW { get; set; }
        public string? ContenedorTransformed { get; set; }
        public string? ContenedorCurated { get; set; }
        public string? ContenedorRejected { get; set; }
        //public string ExtencionArchivosOrigen { get; set; }
        public List<string> NombresArchivosN { get; set; }
    }
}

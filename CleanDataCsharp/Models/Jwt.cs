using System.Security.Claims;

namespace CleanDataCsharp.Models
{
    public class Jwt
    {
        ResponsesModel response = new ResponsesModel();
        public string key { get; set; }
        public string Issuer { get; set; }
        public string Audience { get; set; }
        public string subject { get; set; }

        public dynamic ValidateTokenAzDL(ClaimsIdentity identity)
        {
            try
            {
                if (identity.Claims.Count() == 0)
                {
                    return new
                    {
                        success = false,
                        message = "verificar que token sea valido",
                        result = ""
                    };
                }                
                var endpointBlob = identity.Claims.FirstOrDefault(X=> X.Type== "endpointBlob").Value;
                return new
                {
                    success = true,
                    message = "token valido",
                    result = endpointBlob
                };
            }
            catch (Exception ex)
            {
                return new
                {
                    success = false,
                    message = "",
                    result = ""
                };
            }
        }
    }
}

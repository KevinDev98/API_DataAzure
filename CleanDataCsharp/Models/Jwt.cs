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

        public dynamic ValidateToken(ClaimsIdentity identity)
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
                var email=identity.Claims.FirstOrDefault(X=> X.Type== "email").Value;
                return new
                {
                    success = true,
                    message = "token valido",
                    result = email
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

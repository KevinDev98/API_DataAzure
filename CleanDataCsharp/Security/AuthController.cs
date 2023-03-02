using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json.Linq;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace CleanDataCsharp.Security
{
    [ApiController]
    [Route("LogIn")]
    public class AuthController : Controller
    {
        public IConfiguration _configuration;
        SecurityClass Security = new SecurityClass();
        public AuthController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        [HttpPost]
        [Route("AuthAz")]
        public dynamic AuthAz([FromBody] AzureCon data)
        {
            //se extrae la informacion del yoque
            var jwt = _configuration.GetSection("Jwtlocal").Get<Jwt>();
            var Az = _configuration.GetSection("ConnectionStrings").Get<AzureCon>();

            // se genera la estructura del token
            var claveClaim = new[]
            {
                new Claim(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub, jwt.subject),
                new Claim(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                new Claim(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Iat, DateTime.UtcNow.ToString()),
                new Claim("keyblob", data.Aplicantsemail),
                new Claim("keyblobEncrypt", Az.Key)
            };
            try
            {
                //se genera la key
                var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwt.key));
                //Se genera el inicio de seion
                var SignIn = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
                //Se crea el token
                var token = new JwtSecurityToken(
                    jwt.Issuer,
                    jwt.Audience,
                    claveClaim,
                    expires: DateTime.Now.AddDays(1),
                    signingCredentials: SignIn
                    );
                return new { 
                    success=true,
                    message="token generado correctamente",
                    result= new JwtSecurityTokenHandler().WriteToken(token)
                };
            }
            catch (Exception ex)
            {
                return new
                {
                    success = true,
                    message = "token NO generado",
                    result = ex.Message
                };
            }            
        }
    }
}

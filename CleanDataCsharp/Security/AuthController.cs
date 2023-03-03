using CleanDataCsharp.Class;
using CleanDataCsharp.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json.Linq;
using System.IdentityModel.Tokens.Jwt;
//using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace CleanDataCsharp.Security
{
    [ApiController]
    [Route("Autent")]
    public class AuthController : Controller
    {
        public IConfiguration _configuration;
        SecurityClass Security = new SecurityClass();
        ResponsesModel response = new ResponsesModel();
        public AuthController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        [HttpPost]
        [Route("AuthAz")]
        public dynamic AuthAz([FromBody] ResponsesModel data)
        {
            int exists = 0;
            var Az = _configuration.GetSection("AzureConf").Get<AzureCon>();
            response.solicitante = data.solicitante;
            string email = "";
            try
            {
                for (int z = 0; z < Az.AplicantsemailAddress.Count; z++)
                {
                    email = Az.AplicantsemailAddress[z];
                    if (data.solicitante == email)
                    {
                        exists = 1;
                        break;
                    }
                }
                if (exists == 0)
                {
                    response.CodeResponse = 400;
                    response.MessageResponse = "usuario invalido o usuario sin permisos";
                }
                else
                {
                    //se extrae la informacion del yoque
                    var jwt = _configuration.GetSection("Jwtlocal").Get<Jwt>();
                    // se genera la estructura del token
                    var claveClaim = new[]
                    {
                        new Claim(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub, jwt.subject),
                        new Claim(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                        new Claim(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Iat, DateTime.UtcNow.ToString()),
                        new Claim("email", data.solicitante)
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
                            expires: DateTime.Now.AddHours(1),
                            signingCredentials: SignIn
                            );
                        response.CodeResponse = 200;
                        var result = new JwtSecurityTokenHandler().WriteToken(token);
                        response.MessageResponse = result;
                        //return new
                        //{
                        //    success = true,
                        //    message = "token generado correctamente",
                        //    result = new JwtSecurityTokenHandler().WriteToken(token)
                        //};
                    }
                    catch (Exception ex)
                    {
                        response.CodeResponse = 400;
                        response.MessageResponse = "error generando el token: " + ex.Message + "_" + ex.InnerException;
                    }
                }
            }
            catch (Exception ex)
            {
                response.CodeResponse = 400;
                response.MessageResponse = "error en el proceso: " + ex.Message + "_" + ex.InnerException;
            }

            return Json(response);
        }
    }
}

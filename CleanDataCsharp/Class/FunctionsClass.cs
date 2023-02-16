using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Text.RegularExpressions;

namespace CleanDataCsharp.Class
{
    public class FunctionsClass
    {
        #region Variables globales
        //Console.WriteLine("Hello, World!");
        List<int> indexerror = new List<int>();
        List<string> error = new List<string>();
        DataTable dataerror = new DataTable();
        DataTable DT_DataSource = new DataTable();
        Boolean isnull = false;
        #endregion
        #region Metodos Generales
        public List<Dictionary<string, object>> ConvertDataTableToDicntionary(DataTable dt)
        {
            List<Dictionary<string, object>> filas = new List<Dictionary<string, object>>();
            Dictionary<string, object> fila;
            foreach (DataRow row in dt.Rows)
            {
                fila = new Dictionary<string, object>();
                foreach (DataColumn col in dt.Columns)
                {
                    fila.Add(col.ColumnName, row[col]);
                }
                filas.Add(fila);
            }

            return filas;
        }
        public string Remove_SpacheWithe(string s)
        {
            try
            {
                s = s.Trim().TrimStart().TrimEnd();//Elimina espacios en blanco
            }
            catch
            {
                s = "Error";
                Console.WriteLine("Error eliminando espacios en blanco");
            }
            return s;
        }
        public string Remove_Special_Characteres(string s)
        {
            List<string> CaracteresEspeciales = new List<string>() {
    "#", "$", "%", "&", "!", "|", "[", "]", "{", "}", "/", "_", ";", ":", ",", "*","(",")"
    };//Lista con los caracteres especiales
            try
            {
                foreach (char z in s)//Recorre cada letra de la palabra
                {
                    for (int k = 0; k < CaracteresEspeciales.Count(); k++)// for para recorrer la lista de caracteres especiales
                    {
                        if (Convert.ToString(z) == CaracteresEspeciales[k].ToString())//Valida si el caracter en curso estata en la lista de caracteres
                        {
                            s = s.Replace(Convert.ToString(z), "");//Si el caracter si esta en la lista, lo elimina
                        }
                    }
                }
            }
            catch
            {
                Console.WriteLine("error eliminando caractares");
            }
            return s;
        }
        public Boolean IsNumeric(string z, int index)
        {
            bool TorF = false;
            try
            {
                int numericValue;
                bool isNumber = int.TryParse(z, out numericValue);//Intenta convertir el string a núero
                if (isNumber == false & numericValue == 0)//Si no se puede comvertir entonces...
                {
                    indexerror.Add(index);//Agrega el indice de la fila a una lista
                    error.Add("dato no númerico:" + z);//Agrega el detalle del error a otra lista
                    TorF = false;
                }
                TorF = true;
            }
            catch // Si cae en alguna excepción, agrega el error a una lista
            {
                indexerror.Add(index);
                error.Add("excepción dato no númerico:" + z);
                TorF = false;
            }
            return TorF;
        }
        public string Change_Date_Format(string dte, int index)
        {
            try
            {
                dte = dte.Replace(" 000000.0000000", "");
                DateTime date = Convert.ToDateTime(dte);//Intenta convertir la fecha a un valor tipo decha
                dte = date.ToString("dd/MM/yyyy"); // Da el formato de fecha
            }
            catch (Exception)
            {
                dte = dte + "-Error fecha";//Si la conversión no se puede realizar devolvera error
                Console.WriteLine("Fecha invalida: " + dte + " el formato debe ser dd/MM/YYYY");
            }
            return dte;
        }
        public void Validate_not_greater_today(string dte, int index)
        {
            try
            {
                DateTime date = Convert.ToDateTime(dte);
                dte = date.ToString("dd/MM/yyyy");
                if (date > DateTime.Now)//Valida que la fecha no sea mayor a hoy
                {
                    indexerror.Add(index);
                    error.Add("No se permiten fechas mayores a hoy: " + dte);
                }
            }
            catch (Exception ex)
            {
                indexerror.Add(index);
                error.Add("Fecha invalida: " + dte + " el formato debe ser dd/MM/YYYY");
                //Console.WriteLine("Fecha invalida: " + dte + " el formato debe ser dd/MM/YYYY");
            }
        }
        #endregion
        #region Validaciones Personalizadas
        public Boolean Validate_Names(string name, int index)
        {
            name = name.Replace(" ", "").Replace("-", "").Replace("'", ""); //Estos valores se omiten para la validación
            bool TorF = true;
            foreach (char v in name)//Revorre cada letra del nombre
            {
                try
                {
                    if (!char.IsLetter(v))//Valida si el caracyer en curso es una letra
                    {
                        indexerror.Add(index);
                        error.Add("Nombre no valido:" + name);
                        TorF = false;
                        break;//se termina el foreach ya que el nombre con un solo número es invalido
                    }
                }
                catch
                {
                    indexerror.Add(index);
                    error.Add("Nombre no valido:" + name);
                    TorF = false;
                    Console.WriteLine("nombre invalido");
                }
            }
            return TorF;
        }
        public Boolean Validate_Phone(string s, int lngt, int index)
        {
            Boolean TorF = true;
            try
            {
                if (s.Length > lngt)//Valida si la longitud del telefono es mayor a la longitud permitida
                {
                    indexerror.Add(index);
                    error.Add("Número telefonico no valido. Longitud mayor a " + lngt);
                    TorF = false;
                }
                foreach (char v in s)//Re corre cada caracter del número tel
                {
                    try
                    {
                        if (!IsNumeric(Convert.ToString(v), index)) //Valida si el caracter en curso es un número
                        {
                            indexerror.Add(index);
                            error.Add("Número telefonico no valido:" + s);
                            TorF = false;
                            //Console.WriteLine("nombre invalido");
                            break;//se termina el foreach ya que el nombre con un solo número es invalido
                        }
                    }
                    catch
                    {
                        indexerror.Add(index);
                        error.Add("Número telefonico no valido:" + s);
                        TorF = false;
                        //Console.WriteLine("nombre invalido");
                        break;//se termina el foreach ya que el nombre con un solo número es invalido
                    }
                }
            }
            catch (Exception ex)
            {
                indexerror.Add(index);
                error.Add("Número telefonico no valido: " + s + "-" + ex.Message);
                TorF = false;
            }
            return TorF;
        }
        public Boolean Validate_Email(string s, int index)
        {
            Boolean TorF = true;
            //string regex = "(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])";
            if (new EmailAddressAttribute().IsValid(s.ToLower()))//Valida si el correo es valido
            {
                int arrobas_count = 0;
                foreach (char v in s)//Cuenta el número de arrobas
                {
                    if (v == '@')
                    {
                        arrobas_count++;
                    }
                }
                if (arrobas_count == 0 || arrobas_count > 1)//Valida el número de arrobas
                {
                    indexerror.Add(index);
                    error.Add("email con formato no valido, numero de @ : " + arrobas_count);
                    TorF = false;
                }
            }
            else
            {
                indexerror.Add(index);
                error.Add("email con formato no valido:" + s);
                TorF = false;
            }
            return TorF;
        }
        public Boolean Validate_RFC(string s, int index)
        {
            Boolean TorF = true;
            //string regex_str = "^([A-ZÑ\x26]{3,4}([0-9]{2})(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-9]|3[0-1]))([A-Z]{3})?$";
            string regex_str = "[A-z]{3}[0-9]{6}[A-z0-9]{3}";
            string Regex2 = "[A-z]{4}[0-9]{6}[A-z0-9]{3}";
            if (!Regex.IsMatch(s, regex_str) || !Regex.IsMatch(s, Regex2))//Valida el RFC
            {
                indexerror.Add(index);
                error.Add("RFC con formato no valido:" + s);
                TorF = false;
            }
            return TorF;
        }
        public Boolean Validate_Amount(string amount, int index)//Valida que el monto no sea menor a 0
        {
            Boolean TorF = true;
            try
            {
                decimal dec_mount = new decimal();
                dec_mount = Convert.ToDecimal(amount);
                if (dec_mount < 0)//Valida que los montos no sean menores a 0
                {
                    indexerror.Add(index);
                    error.Add("monto con formato no valido:" + amount);
                    TorF = false;
                }
            }
            catch (Exception ex)
            {
                indexerror.Add(index);
                error.Add("monto con formato no valido:" + amount);
                TorF = false;
            }
            return TorF;
        }
        public Decimal FormatDecimal(string z)//Define el número de decimales
        {
            Decimal dec_ = new Decimal();
            dec_ = Convert.ToDecimal(z);
            dec_ = decimal.Round(dec_, 2);
            return dec_;
        }
        #endregion
        #region FormateoTablas
        public void CopyHeaders(DataTable dt, DataTable csv)
        {
            int ColumnasHeader = dt.Columns.Count;
            //Copia la estructura de la tabla original a la tabla de errores
            for (var i = 0; i <= ColumnasHeader - 1; i++)
                csv.Columns.Add(dt.Columns[i].ColumnName);
        }
        public DataTable DeleteDirtyRows(DataTable dt) //Elimina las filas sucias de una tabla
        {
            string data = "";
            int idx = 0;
            DataRow NewR;
            DataTable CleanDT = new DataTable();
            CopyHeaders(dt, CleanDT);
            try
            {
                for (int z = 0; z < dt.Rows.Count; z++)
                {
                    data = dt.Rows[z][0].ToString();
                    idx = dt.Rows.IndexOf(dt.Rows[z]);
                    NewR = dt.Rows[z];
                    if (!data.Contains("ERRORONROW"))
                    {
                        CleanDT.ImportRow(NewR);
                        CleanDT.AcceptChanges();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERRORONROW dirty rows" + ex.Message);
            }
            return CleanDT;
        }
        public void ControlErrores(DataTable DtErrores, int z)
        {
            DataRow row;
            int idxRow = 0;
            int LastCol = 0;
            idxRow = indexerror[0];//obtiene el indice de la fila con el error
            row = DtErrores.Rows[idxRow];//guarda toda la fila del error
            indexerror.RemoveAt(0);//Elimina el indice de la fila erronea de la lista

            dataerror.ImportRow(row);//agraga la fila erronea al data table de error
            dataerror.AcceptChanges();//Guardar cambios

            idxRow = dataerror.Rows.Count - 1; //Obtiene indice la fila actual de tabla errores 
            LastCol = dataerror.Columns.Count - 1;//Obtiene el indice de la columna del detalle del error
            dataerror.Rows[idxRow][LastCol] = error[0];//Define el detalle del error
            dataerror.AcceptChanges();//Guardar cambios
            error.RemoveAt(0);//Elimina el indice de la fila de errores

            DtErrores.Rows[z][0] = "ERRORONROW"; //Define fila como erronea
            DtErrores.AcceptChanges();
        }        
        #endregion
        #region Reglas aplicadas 
        public DataTable CleanDataTableClientes(DataTable dt)
        {
            string data = "";
            dataerror = new DataTable();
            dataerror.Columns.Add("Detalle error");//Agrega la columna del error
            for (int z = 0; z < dt.Rows.Count; z++)
            {
                for (int s = 0; s < dt.Columns.Count; s++)
                {
                    try
                    {
                        if (dt.Rows[z][s] == null)//Valida si la celda es null
                        {
                            isnull = true;
                            indexerror.Add(z);
                            error.Add("Se encontro un valor nulo en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                            ControlErrores(dt, z);
                        }
                        else
                        {
                            data = dt.Rows[z][s].ToString();//DT.ROWS[FILA][COLUMNA]
                            if (string.IsNullOrEmpty(data))
                            {
                                isnull = true;
                                indexerror.Add(z);
                                error.Add("Se encontro un valor vacio en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                                ControlErrores(dt, z);
                            }
                        }
                        if (!isnull)//si no es null entonces continua con el proceso
                        {
                            if (s == 0)
                            {
                                if (IsNumeric(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("ID NO NUMERICO:" + data);
                                }
                            }
                            if (s == 1)
                            {
                                if (Validate_Names(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("MOMBRE CON FORMATO INCORRECTO:" + data);
                                }
                            }
                            if (s == 2)
                            {
                                if (Validate_RFC(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("RFC CON FORMATO INCORRECTO:" + data);
                                }
                            }
                            if (s == 3 || s == 8)
                            {
                                try
                                {
                                    dt.Rows[z][s] = Change_Date_Format(data, z);
                                    if (dt.Rows[z][s].ToString().Contains("Error") || data.Contains("DE"))
                                    {
                                        indexerror.Add(z);
                                        error.Add("Fecha invalida: " + data + " el formato debe ser dd/MM/YYYY");
                                        ControlErrores(dt, z);
                                    }
                                    else
                                    {
                                        Validate_not_greater_today(dt.Rows[z][s].ToString(), z);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("FECHA CON FORMATO INCORRECTO:" + data + "-" + ex.Message);
                                }
                            }
                            if (s == 4)
                            {
                                if (Validate_Email(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("EMAIL CON FORMATO INCORRECTO:" + data);
                                }
                            }
                            if (s == 5)
                            {
                                if (Validate_Phone(data, 10, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("NÚMERO CON FORMATO INCORRECTO:" + data);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("error limpiando datos clientes: " + ex.Message + "s:" + s + "z" + z);
                    }
                }
            }
            return dt;
        }

        public DataTable CleanDataTableProductos(DataTable dt)
        {
            string data = "";
            dataerror = new DataTable();
            dataerror.Columns.Add("Detalle error");//Agrega la columna del error
            for (int z = 0; z < dt.Rows.Count; z++)//Filas
            {
                for (int s = 0; s < dt.Columns.Count; s++)
                {
                    isnull = false;
                    try
                    {
                        if (dt.Rows[z][s] == null)//Valida si la celda es null o vacio
                        {
                            isnull = true;
                            indexerror.Add(z);
                            error.Add("Se encontro un valor nulo en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                            ControlErrores(dt, z);
                        }
                        else
                        {
                            data = dt.Rows[z][s].ToString();//DT.ROWS[FILA][COLUMNA]
                            if (string.IsNullOrEmpty(data))
                            {
                                isnull = true;
                                indexerror.Add(z);
                                try
                                {
                                    error.Add("Se encontro un valor vacio en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                                }
                                catch (Exception)
                                {
                                    error.Add("Se encontro una fila vacia en la fila " + (z + 1));
                                }
                                ControlErrores(dt, z);
                            }
                        }
                        if (!isnull)//si no es null entonces continua con el proceso
                        {
                            if (s == 0)
                            {
                                if (IsNumeric(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("ID NO NUMERICO:" + data);
                                }
                            }
                            else if (s == 4)
                            {
                                //if (data.Contains("-"))
                                //{
                                //    Console.WriteLine("valor negativo");
                                //}
                                if (Validate_Amount(data, z))//Si el monto no es menor a 0, le da el formato correspondiente
                                {
                                    dt.Rows[z][s] = FormatDecimal(data);
                                }
                                else
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("valor negativo");
                                }
                            }
                            else if (s == 5)
                            {
                                try
                                {
                                    dt.Rows[z][s] = Change_Date_Format(data, z);
                                    if (dt.Rows[z][s].ToString().Contains("Error") || data.Contains("DE"))
                                    {
                                        indexerror.Add(z);
                                        error.Add("Fecha invalida: " + data + " el formato debe ser dd/MM/YYYY");
                                        ControlErrores(dt, z);
                                    }
                                    else
                                    {
                                        Validate_not_greater_today(dt.Rows[z][s].ToString(), z);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("FECHA CON FORMATO INCORRECTO:" + data + "-" + ex.Message);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("error limpiando datos productos: " + ex.Message + "s:" + s + "z" + z);
                    }
                }
            }
            return dt;
        }

        public DataTable CleanDataTableSucursales(DataTable dt)
        {
            string data = "";
            dataerror = new DataTable();
            dataerror.Columns.Add("Detalle error");//Agrega la columna del error
            for (int z = 0; z < dt.Rows.Count; z++)//Filas
            {
                for (int s = 0; s < dt.Columns.Count; s++)
                {
                    isnull = false;
                    try
                    {
                        if (dt.Rows[z][s] == null)//Valida si la celda es null o vacio
                        {
                            isnull = true;
                            indexerror.Add(z);
                            error.Add("Se encontro un valor nulo en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                            ControlErrores(dt, z);
                        }
                        else
                        {
                            data = dt.Rows[z][s].ToString();//DT.ROWS[FILA][COLUMNA]
                            if (string.IsNullOrEmpty(data))
                            {
                                isnull = true;
                                indexerror.Add(z);
                                error.Add("Se encontro un valor vacio en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                                ControlErrores(dt, z);
                            }
                        }
                        if (!isnull)//si no es null entonces continua con el proceso
                        {
                            if (s == 0)
                            {
                                if (IsNumeric(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("ID NO NUMERICO:" + data);
                                }
                            }
                            else if (s == 5)
                            {
                                try
                                {
                                    dt.Rows[z][s] = Change_Date_Format(data, z);
                                    if (dt.Rows[z][s].ToString().Contains("Error") || data.Contains("DE"))
                                    {
                                        indexerror.Add(z);
                                        error.Add("Fecha invalida: " + data + " el formato debe ser dd/MM/YYYY");
                                        ControlErrores(dt, z);
                                    }
                                    else
                                    {
                                        Validate_not_greater_today(dt.Rows[z][s].ToString(), z);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("FECHA CON FORMATO INCORRECTO:" + data + "-" + ex.Message);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("error limpiando datos clientes: " + ex.Message + "s:" + s + "z" + z);
                    }
                }
            }
            return dt;
        }

        public DataTable CleanDataTableVentas(DataTable dt)
        {
            string data = "";
            dataerror = new DataTable();
            dataerror.Columns.Add("Detalle error");//Agrega la columna del error
            for (int z = 0; z < dt.Rows.Count; z++)//Filas
            {
                for (int s = 0; s < dt.Columns.Count; s++)
                {
                    isnull = false;
                    try
                    {
                        if (dt.Rows[z][s] == null)//Valida si la celda es null o vacio
                        {
                            isnull = true;
                            indexerror.Add(z);
                            error.Add("Se encontro un valor nulo en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                            ControlErrores(dt, z);
                        }
                        else
                        {
                            data = dt.Rows[z][s].ToString();//DT.ROWS[FILA][COLUMNA]
                            if (string.IsNullOrEmpty(data))
                            {
                                isnull = true;
                                indexerror.Add(z);
                                error.Add("Se encontro un valor vacio en la columna " + dt.Columns[s].ColumnName + " en la fila " + (z + 1));
                                ControlErrores(dt, z);
                            }
                        }
                        if (!isnull)//si no es null entonces continua con el proceso
                        {
                            if (s == 0 || s > 5)
                            {
                                if (IsNumeric(data, z) == false)
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("ID NO NUMERICO:" + data);
                                }
                            }
                            else if (s > 1 & s < 6)
                            {
                                //if (data.Contains("-"))
                                //{
                                //    Console.WriteLine("valor negativo");
                                //}
                                if (Validate_Amount(data, z))//Si el monto no es menor a 0, le da el formato correspondiente
                                {
                                    dt.Rows[z][s] = FormatDecimal(data);
                                }
                                else
                                {
                                    ControlErrores(dt, z);
                                    Console.WriteLine("valor negativo");
                                }
                            }
                            else if (s == 1)
                            {
                                try
                                {
                                    dt.Rows[z][s] = Change_Date_Format(data, z);
                                    if (dt.Rows[z][s].ToString().Contains("Error") || data.Contains("DE"))
                                    {
                                        indexerror.Add(z);
                                        error.Add("Fecha invalida: " + data + " el formato debe ser dd/MM/YYYY");
                                        ControlErrores(dt, z);
                                    }
                                    else
                                    {
                                        Validate_not_greater_today(dt.Rows[z][s].ToString(), z);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("FECHA CON FORMATO INCORRECTO:" + data + "-" + ex.Message);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("error limpiando datos Ventas: " + ex.Message + "s:" + s + "z" + z);
                    }
                }
            }
            return dt;
        }
        #endregion
    }
}

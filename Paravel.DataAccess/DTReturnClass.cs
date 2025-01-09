using System.Data;

namespace Paravel.DataAccess;

public class DTReturnClass
{
    public DataTable? Datatable { get; set; }
    public bool Success { get; set; } = true;
    public string Message { get; set; } = string.Empty;
    public string Techmessage { get; set; } = string.Empty;

}

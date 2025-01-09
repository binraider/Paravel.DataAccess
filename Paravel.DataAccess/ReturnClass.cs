using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Paravel.DataAccess;

public class ReturnClass
{
    public bool Success { get; set; } = true;
    public string Message { get; set; } = string.Empty;
    public string Techmessage { get; set; } = string.Empty;
    public int Intvar { get; set; } = 0;
    public long Longvar { get; set; } = 0;
    public double Doublevar { get; set; } = 0;

    public void SetFailureMessage(string message)
    {
        Success = false;
        Message = message;
    }
    public void SetFailureMessage(string message, string techmessage)
    {
        Success = false;
        Message = message;
        Techmessage = techmessage;
    }
}

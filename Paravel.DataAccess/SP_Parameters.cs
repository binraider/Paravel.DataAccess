using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Paravel.DataAccess;

public class SP_Parameters : IEnumerable
{
    private List<SqlParameter> Items;
    private SqlParameter sqlparam;

    public SP_Parameters()
    {
        Items = new List<SqlParameter>();
    }
    public void Add(SqlParameter para)
    {
        Items.Add(para);
    }
    public void Add(string parametername, SqlDbType dbType, int paramsize, ParameterDirection direction, object paramvalue)
    {
        sqlparam = new SqlParameter(parametername, dbType);
        sqlparam.Direction = direction;
        if (paramsize != 0)
            sqlparam.Size = paramsize;
        if (paramvalue != null)
            sqlparam.Value = paramvalue;
        Items.Add(sqlparam);
    }

    public IEnumerator GetEnumerator()
    {
        return this.Items.GetEnumerator();
    }
}
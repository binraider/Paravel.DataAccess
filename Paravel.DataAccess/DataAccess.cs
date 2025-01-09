using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Paravel.DataAccess;

public interface IDataAccess
{
    ReturnClass ExecProcIntResultParams(string query, SP_Parameters p);

    DTReturnClass ExecProcRS(string procname);
    DTReturnClass ExecProcRSParams(string procname, SP_Parameters p);

    ReturnClass ExecProcVoid(string procname);
    Task<ReturnClass> ExecProcVoidAsync(string procname);
    Task<ReturnClass> ExecProcVoidAsync(string procname, CancellationToken cancellationToken);

    ReturnClass ExecProcVoidParams(string procname, SP_Parameters p);
    Task<ReturnClass> ExecProcVoidParamsAsync(string procname, SP_Parameters p);
    Task<ReturnClass> ExecProcVoidParamsAsync(string procname, SP_Parameters p, CancellationToken cancellationToken);

    ReturnClass ExecSql(string strQuery);
    Task<ReturnClass> ExecSqlAsync(string strQuery);
    Task<ReturnClass> ExecSqlAsync(string strQuery, CancellationToken cancellationToken);

    ReturnClass ExecSqlParams(string q, SP_Parameters p);
    Task<ReturnClass> ExecSqlParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> ExecSqlParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);

    DTReturnClass GetDataTable(string queryString);
    Task<DTReturnClass> GetDataTableAsync(string queryString);

    DTReturnClass GetDataTableParams(string queryString, SP_Parameters p);
    Task<DTReturnClass> GetDataTableParamsAsync(string queryString, SP_Parameters p);

    DTReturnClass GetDataTableProc(string procname);
    Task<DTReturnClass> GetDataTableProcAsync(string procname);

    DTReturnClass GetDataTableProcParams(string procname, SP_Parameters p);
    Task<DTReturnClass> GetDataTableProcParamsAsync(string procname, SP_Parameters p);

    Task<ReturnClass> GetDoubleScalarAsync(string strQuery);
    Task<ReturnClass> GetDoubleScalarAsync(string strQuery, CancellationToken cancellationToken);

    ReturnClass GetDoubleScalarParams(string query, SP_Parameters p);
    Task<ReturnClass> GetDoubleScalarParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetDoubleScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);

    Task<ReturnClass> GetDoubleScalarProcAsync(string strQuery);
    Task<ReturnClass> GetDoubleScalarProcAsync(string strQuery, CancellationToken cancellationToken);

    ReturnClass GetDoubleScalarProcParams(string procname, SP_Parameters p);
    Task<ReturnClass> GetDoubleScalarProcParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetDoubleScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);

    ReturnClass GetIntScalar(string query);
    Task<ReturnClass> GetIntScalarAsync(string strQuery);
    Task<ReturnClass> GetIntScalarAsync(string strQuery, CancellationToken cancellationToken);
    ReturnClass GetIntScalarParams(string query, SP_Parameters p);
    Task<ReturnClass> GetIntScalarParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetIntScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);
    Task<ReturnClass> GetIntScalarProcAsync(string strQuery);
    Task<ReturnClass> GetIntScalarProcAsync(string strQuery, CancellationToken cancellationToken);
    ReturnClass GetIntScalarProcParams(string procname, SP_Parameters p);
    Task<ReturnClass> GetIntScalarProcParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetIntScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);
    ReturnClass GetLongScalar(string query);
    Task<ReturnClass> GetLongScalarAsync(string strQuery);
    Task<ReturnClass> GetLongScalarAsync(string strQuery, CancellationToken cancellationToken);
    ReturnClass GetLongScalarParams(string query, SP_Parameters p);
    Task<ReturnClass> GetLongScalarParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetLongScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);
    Task<ReturnClass> GetLongScalarProcAsync(string strQuery);
    Task<ReturnClass> GetLongScalarProcAsync(string strQuery, CancellationToken cancellationToken);
    ReturnClass GetLongScalarProcParams(string procname, SP_Parameters p);
    Task<ReturnClass> GetLongScalarProcParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetLongScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);
    ReturnClass GetStringScalar(string query);
    Task<ReturnClass> GetStringScalarAsync(string strQuery);
    Task<ReturnClass> GetStringScalarAsync(string strQuery, CancellationToken cancellationToken);
    ReturnClass GetStringScalarParams(string query, SP_Parameters p);
    Task<ReturnClass> GetStringScalarParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetStringScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);
    Task<ReturnClass> GetStringScalarProcAsync(string strQuery);
    Task<ReturnClass> GetStringScalarProcAsync(string strQuery, CancellationToken cancellationToken);
    Task<ReturnClass> GetStringScalarProcParamsAsync(string strQuery, SP_Parameters p);
    Task<ReturnClass> GetStringScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken);

}

public class DataAccess : IDataAccess
{

    //private bool disposed = false;
    //private SqlConnection conn = null;
    private string connectionstring = "";
    private int commandtimeout = 0;

    public DataAccess(string m_connectionstring)
    {
        if (!string.IsNullOrEmpty(m_connectionstring))
        {
            connectionstring = m_connectionstring;
        }
    }

    public DataAccess(string m_connectionstring, int m_commandtimeout)
    {
        try
        {
            commandtimeout = m_commandtimeout;
            connectionstring = m_connectionstring;
        }
        catch (SqlException ex)
        {

        }
        catch (Exception ex)
        {

        }
    }


    #region Async Void Methods
    public async Task<ReturnClass> ExecSqlAsync(string strQuery)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(strQuery, null, false);
        return outcome;
    }

    public async Task<ReturnClass> ExecSqlParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(strQuery, p, false);
        return outcome;
    }

    public async Task<ReturnClass> ExecProcVoidAsync(string procname)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(procname, null, true);
        return outcome;
    }

    public async Task<ReturnClass> ExecProcVoidParamsAsync(string procname, SP_Parameters p)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(procname, p, true);
        return outcome;
    }

    private async Task<ReturnClass> ExecSqlAsyncMethods(string strQuery, SP_Parameters p, bool isProc)
    {
        ReturnClass outcome = new ReturnClass();
        int result = 0;
        string errormsg = "";
        bool isError = false;
        using (var conn = new SqlConnection(connectionstring))
        {
            await conn.OpenAsync();
            using (var tran = conn.BeginTransaction())
            {
                using (var command = new SqlCommand(strQuery, conn, tran))
                {
                    command.CommandTimeout = commandtimeout;
                    if (isProc)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandType = CommandType.Text;
                    }

                    if (p != null)
                    {
                        foreach (SqlParameter objparam1 in p)
                        {
                            command.Parameters.Add(objparam1);
                        }
                    }
                    try
                    {
                        result = await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        errormsg = ex.ToString();
                        isError = true;
                        tran.Rollback();
                        throw;
                    }
                    tran.Commit();
                }
            }
        }
        if (isError)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "GetDataTable error. Query is[" + strQuery + "] Error:[" + errormsg + "]";
        }
        outcome.Intvar = result;

        return outcome;
    }


    #endregion

    #region Async Void Methods with CancellationToken

    public async Task<ReturnClass> ExecSqlAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(strQuery, null, false, cancellationToken);
        return outcome;
    }

    public async Task<ReturnClass> ExecSqlParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(strQuery, p, false, cancellationToken);
        return outcome;
    }

    public async Task<ReturnClass> ExecProcVoidAsync(string procname, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(procname, null, true, cancellationToken);
        return outcome;
    }

    public async Task<ReturnClass> ExecProcVoidParamsAsync(string procname, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await ExecSqlAsyncMethods(procname, p, true, cancellationToken);
        return outcome;
    }

    // TODO https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlconnection.begintransaction?view=sqlclient-dotnet-core-1.1

    private async Task<ReturnClass> ExecSqlAsyncMethods(string strQuery, SP_Parameters p, bool isProc, CancellationToken token)
    {
        ReturnClass outcome = new ReturnClass();
        int result = 0;
        string errormsg = "";
        bool isError = false;
        bool isCancelled = false;
        using (var conn = new SqlConnection(connectionstring))
        {
            await conn.OpenAsync();
            using (var tran = conn.BeginTransaction())
            {
                using (var command = new SqlCommand(strQuery, conn, tran))
                {
                    command.CommandTimeout = commandtimeout;
                    if (isProc)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandType = CommandType.Text;
                    }

                    if (p != null)
                    {
                        foreach (SqlParameter objparam1 in p)
                        {
                            command.Parameters.Add(objparam1);
                        }
                    }
                    try
                    {
                        result = await command.ExecuteNonQueryAsync(token);
                    }
                    catch (TaskCanceledException ex)
                    {
                        isCancelled = true;
                        tran.Rollback();
                    }
                    catch (Exception ex)
                    {
                        errormsg = ex.ToString();
                        isError = true;
                        tran.Rollback();
                    }
                    tran.Commit();
                }
            }
        }
        if (isError)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "GetDataTable error. Query is[" + strQuery + "] Error:[" + errormsg + "]";
        }
        if (isCancelled)
        {
            outcome.Success = false;
            outcome.Message = "The operation was cancelled by the calling code";
        }
        outcome.Intvar = result;

        return outcome;
    }

    #endregion

    #region Async DataSet Methods


    public async Task<DTReturnClass> GetDataTableAsync(string queryString)
    {
        return await AsyncDataTableMethods(queryString, null, false);
    }

    public async Task<DTReturnClass> GetDataTableParamsAsync(string queryString, SP_Parameters p)
    {
        return await AsyncDataTableMethods(queryString, p, false);
    }

    public async Task<DTReturnClass> GetDataTableProcAsync(string procname)
    {
        return await AsyncDataTableMethods(procname, null, true);
    }

    public async Task<DTReturnClass> GetDataTableProcParamsAsync(string procname, SP_Parameters p)
    {
        return await AsyncDataTableMethods(procname, p, true);
    }

    private async Task<DTReturnClass> AsyncDataTableMethods(string strQuery, SP_Parameters p, bool isProc)
    {
        DTReturnClass outcome = new DTReturnClass();
        DataTable dt = new DataTable();

        string errormsg = "";
        bool isError = false;
        using (var conn = new SqlConnection(connectionstring))
        {
            await conn.OpenAsync();
            using (var command = new SqlCommand(strQuery, conn))
            {
                command.CommandTimeout = commandtimeout;
                if (isProc)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                if (p != null)
                {
                    foreach (SqlParameter objparam1 in p)
                    {
                        command.Parameters.Add(objparam1);
                    }
                }

                try
                {
                    using (SqlDataAdapter sda = new SqlDataAdapter(command))
                    {
                        sda.Fill(dt);
                    }

                }
                catch (Exception ex)
                {
                    errormsg = ex.ToString();
                    isError = true;
                }
            }
        }


        if (isError)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "GetDataTable error. Query is[" + strQuery + "] Error:[" + errormsg + "]";
        }
        else
        {
            if (dt != null)
            {
                outcome.Datatable = dt;
            }
            else
            {
                outcome.Success = false;
                outcome.Message = "A query Failed. Please see logs for exact error";
                outcome.Techmessage = "GetDataTable error. Datatable is null";
            }
        }

        return outcome;
    }


    #endregion

    #region Async Scalar Methods

    public async Task<ReturnClass> GetStringScalarAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.String);
        return outcome;
    }
    public async Task<ReturnClass> GetStringScalarParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.String);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.Int);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.Int);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.Long);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.Long);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.Double);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.Double);
        return outcome;
    }

    public async Task<ReturnClass> GetStringScalarProcAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.String);
        return outcome;
    }
    public async Task<ReturnClass> GetStringScalarProcParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.String);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarProcAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.Int);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarProcParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.Int);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarProcAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.Long);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarProcParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.Long);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarProcAsync(string strQuery)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.Double);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarProcParamsAsync(string strQuery, SP_Parameters p)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.Double);
        return outcome;
    }

    private enum ScalarType
    {
        None = 0,
        String = 1,
        Int = 2,
        Long = 3,
        Double = 4
    }
    private async Task<ReturnClass> GetScalarAsyncMethods(string strQuery, SP_Parameters p, bool isProc, ScalarType sctype)
    {
        ReturnClass outcome = new ReturnClass();

        string errormsg = "";
        bool isError = false;
        using (var conn = new SqlConnection(connectionstring))
        {
            await conn.OpenAsync();
            using (var command = new SqlCommand(strQuery, conn))
            {
                command.CommandTimeout = commandtimeout;

                if (isProc)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                if (p != null)
                {
                    foreach (SqlParameter objparam1 in p)
                    {
                        command.Parameters.Add(objparam1);
                    }
                }

                try
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            switch (sctype)
                            {
                                case ScalarType.String:
                                    outcome.Message = reader[0].ToString();
                                    break;
                                case ScalarType.Int:
                                    outcome.Intvar = (int)reader[0];
                                    break;
                                case ScalarType.Long:
                                    outcome.Longvar = (long)reader[0];
                                    break;
                                case ScalarType.Double:
                                    outcome.Doublevar = (double)reader[0];
                                    break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    errormsg = ex.ToString();
                    isError = true;
                }
            }
        }

        if (isError)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "GetDataTable error. Query is[" + strQuery + "] Error:[" + errormsg + "]";
        }

        return outcome;
    }


    #endregion

    #region Async Scalar Methods With Cancellation token

    private async Task<ReturnClass> GetScalarAsyncMethods(string strQuery, SP_Parameters p, bool isProc, ScalarType sctype, CancellationToken token)
    {
        ReturnClass outcome = new ReturnClass();

        string errormsg = "";
        bool isError = false;
        bool isCancelled = false;
        using (var conn = new SqlConnection(connectionstring))
        {
            await conn.OpenAsync();
            using (var command = new SqlCommand(strQuery, conn))
            {
                command.CommandTimeout = commandtimeout;

                if (isProc)
                {
                    command.CommandType = CommandType.StoredProcedure;
                }
                else
                {
                    command.CommandType = CommandType.Text;
                }

                if (p != null)
                {
                    foreach (SqlParameter objparam1 in p)
                    {
                        command.Parameters.Add(objparam1);
                    }
                }

                try
                {
                    using (var reader = await command.ExecuteReaderAsync(token))
                    {
                        if (await reader.ReadAsync())
                        {
                            switch (sctype)
                            {
                                case ScalarType.String:
                                    outcome.Message = reader[0].ToString();
                                    break;
                                case ScalarType.Int:
                                    outcome.Intvar = (int)reader[0];
                                    break;
                                case ScalarType.Long:
                                    outcome.Longvar = (long)reader[0];
                                    break;
                                case ScalarType.Double:
                                    outcome.Doublevar = (double)reader[0];
                                    break;
                            }
                        }
                    }
                }
                catch (TaskCanceledException ex)
                {
                    isCancelled = true;

                }
                catch (Exception ex)
                {
                    errormsg = ex.ToString();
                    isError = true;
                }
            }
        }

        if (isError)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "GetDataTable error. Query is[" + strQuery + "] Error:[" + errormsg + "]";
        }
        if (isCancelled)
        {
            outcome.Success = false;
            outcome.Message = "The operation was cancelled by the calling code";
        }
        return outcome;
    }

    public async Task<ReturnClass> GetStringScalarAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.String, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetStringScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.String, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.Int, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.Int, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.Long, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.Long, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, false, ScalarType.Double, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, false, ScalarType.Double, cancellationToken);
        return outcome;
    }

    public async Task<ReturnClass> GetStringScalarProcAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.String, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetStringScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.String, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarProcAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.Int, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetIntScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.Int, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarProcAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.Long, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetLongScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.Long, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarProcAsync(string strQuery, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, null, true, ScalarType.Double, cancellationToken);
        return outcome;
    }
    public async Task<ReturnClass> GetDoubleScalarProcParamsAsync(string strQuery, SP_Parameters p, CancellationToken cancellationToken)
    {
        ReturnClass outcome = await GetScalarAsyncMethods(strQuery, p, true, ScalarType.Double, cancellationToken);
        return outcome;
    }





    #endregion

    #region Sync void methods
    public ReturnClass ExecSql(string strQuery)
    {
        return ExecSqlProcVoidMethods(strQuery, false, null);
    }

    public ReturnClass ExecSqlParams(string q, SP_Parameters p)
    {
        return ExecSqlProcVoidMethods(q, false, p);
    }

    public ReturnClass ExecProcVoid(string procname)
    {
        return ExecSqlProcVoidMethods(procname, true, null);
    }

    public ReturnClass ExecProcVoidParams(string procname, SP_Parameters p)
    {
        return ExecSqlProcVoidMethods(procname, true, p);
    }

    private ReturnClass ExecSqlProcVoidMethods(string strQuery, bool isProc, SP_Parameters p)
    {

        ReturnClass outcome = new ReturnClass();
        int results = 0;
        try
        {
            using (var conn = new SqlConnection(connectionstring))
            {
                conn.Open();
                using (var command = new SqlCommand(strQuery, conn))
                {
                    command.CommandTimeout = commandtimeout;

                    if (isProc)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandType = CommandType.Text;
                    }

                    if (p != null)
                    {
                        foreach (SqlParameter objparam1 in p)
                        {
                            command.Parameters.Add(objparam1);
                        }
                    }

                    results = command.ExecuteNonQuery();
                }
            }
        }
        catch (Exception ex)
        {
            outcome.SetFailureMessage("An update query Failed. Please see logs for exact error", "ExecSql error. Query is[" + strQuery + "] Error:[" + ex.Message + "]");
            results = -1;
        }
        outcome.Intvar = results;
        return outcome;

    }

    #endregion

    #region Sync Scalar methods

    public ReturnClass GetStringScalar(string query)
    {
        return SyncScalarMethods(query, null, false, ScalarType.String);
    }

    public ReturnClass GetStringScalarParams(string query, SP_Parameters p)
    {
        return SyncScalarMethods(query, p, false, ScalarType.String);
    }

    public ReturnClass GetIntScalar(string query)
    {
        return SyncScalarMethods(query, null, false, ScalarType.Int);
    }

    public ReturnClass GetIntScalarParams(string query, SP_Parameters p)
    {
        return SyncScalarMethods(query, p, false, ScalarType.Int);
    }

    public ReturnClass GetLongScalar(string query)
    {
        return SyncScalarMethods(query, null, false, ScalarType.Long);
    }

    public ReturnClass GetLongScalarParams(string query, SP_Parameters p)
    {
        return SyncScalarMethods(query, p, false, ScalarType.Long);
    }

    public ReturnClass GetDoubleScalarParams(string query, SP_Parameters p)
    {
        return SyncScalarMethods(query, p, false, ScalarType.Double);
    }

    public ReturnClass GetLongScalarProcParams(string procname, SP_Parameters p)
    {
        return SyncScalarMethods(procname, p, true, ScalarType.Long);
    }

    public ReturnClass GetDoubleScalarProcParams(string procname, SP_Parameters p)
    {
        return SyncScalarMethods(procname, p, true, ScalarType.Double);
    }
    public ReturnClass GetIntScalarProcParams(string procname, SP_Parameters p)
    {
        return SyncScalarMethods(procname, p, true, ScalarType.Int);
    }
    public ReturnClass ExecProcIntResultParams(string query, SP_Parameters p)
    {
        return SyncScalarMethods(query, p, true, ScalarType.Int);
    }

    private ReturnClass SyncScalarMethods(string strQuery, SP_Parameters p, bool isProc, ScalarType sctype)
    {
        ReturnClass outcome = new ReturnClass();

        try
        {
            using (var conn = new SqlConnection(connectionstring))
            {
                conn.Open();
                using (var command = new SqlCommand(strQuery, conn))
                {
                    command.CommandTimeout = commandtimeout;

                    if (isProc)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandType = CommandType.Text;
                    }

                    if (p != null)
                    {
                        foreach (SqlParameter objparam1 in p)
                        {
                            command.Parameters.Add(objparam1);
                        }
                    }

                    try
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                switch (sctype)
                                {
                                    case ScalarType.String:
                                        outcome.Message = reader[0].ToString();
                                        break;
                                    case ScalarType.Int:
                                        outcome.Intvar = (int)reader[0];
                                        break;
                                    case ScalarType.Long:
                                        outcome.Longvar = (long)reader[0];
                                        break;
                                    case ScalarType.Double:
                                        outcome.Doublevar = (double)reader[0];
                                        break;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        outcome.SetFailureMessage("A query Failed. Please see logs for exact error", "SyncScalarMethods Error Query is[" + strQuery + "] Error:[" + ex.ToString() + "]");
                    }
                }
            }
        }
        catch (SqlException ex)
        {
            outcome.SetFailureMessage("A query Failed. Please see logs for exact error", "SyncScalarMethods Error Query is[" + strQuery + "] Sql Error:[" + ex.ToString() + "]");
        }
        catch (Exception ex)
        {
            outcome.SetFailureMessage("A query Failed. Please see logs for exact error", "SyncScalarMethods Error Query is[" + strQuery + "] Error:[" + ex.ToString() + "]");
        }
        return outcome;
    }

    #endregion

    #region Sync DataSet methods


    public DTReturnClass GetDataTable(string queryString)
    {
        return SyncDatatableMethods(queryString, false, null);
    }

    public DTReturnClass GetDataTableParams(string queryString, SP_Parameters p)
    {
        return SyncDatatableMethods(queryString, false, p);
    }

    public DTReturnClass GetDataTableProc(string procname)
    {
        return SyncDatatableMethods(procname, true, null);
    }

    public DTReturnClass GetDataTableProcParams(string procname, SP_Parameters p)
    {
        return SyncDatatableMethods(procname, true, p);
    }

    public DTReturnClass ExecProcRS(string procname)
    {
        return SyncDatatableMethods(procname, true, null);
    }

    public DTReturnClass ExecProcRSParams(string procname, SP_Parameters p)
    {
        return SyncDatatableMethods(procname, true, p);
    }

    private DTReturnClass SyncDatatableMethods(string strQuery, bool isProc, SP_Parameters p)
    {
        DTReturnClass outcome = new DTReturnClass();
        DataTable dt = new DataTable();

        try
        {
            using (var conn = new SqlConnection(connectionstring))
            {
                conn.Open();
                using (var command = new SqlCommand(strQuery, conn))
                {
                    command.CommandTimeout = commandtimeout;
                    if (isProc)
                    {
                        command.CommandType = CommandType.StoredProcedure;
                    }
                    else
                    {
                        command.CommandType = CommandType.Text;
                    }

                    if (p != null)
                    {
                        foreach (SqlParameter objparam1 in p)
                        {
                            command.Parameters.Add(objparam1);
                        }
                    }

                    try
                    {
                        using (SqlDataAdapter sda = new SqlDataAdapter(command))
                        {
                            sda.Fill(dt);
                        }

                        outcome.Datatable = dt;

                    }
                    catch (Exception ex)
                    {
                        outcome.Success = false;
                        outcome.Message = "A query Failed. Please see logs for exact error";
                        outcome.Techmessage = "SyncDatatableMethods error. Query is[" + strQuery + "] Error:[" + ex.ToString() + "]";
                    }
                }
            }
        }
        catch (SqlException ex)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "SyncDatatableMethods error. Query is[" + strQuery + "] Error:[" + ex.ToString() + "]";
        }
        catch (Exception ex)
        {
            outcome.Success = false;
            outcome.Message = "A query Failed. Please see logs for exact error";
            outcome.Techmessage = "SyncDatatableMethods error. Query is[" + strQuery + "] Error:[" + ex.ToString() + "]";
        }

        return outcome;
    }
    #endregion

}

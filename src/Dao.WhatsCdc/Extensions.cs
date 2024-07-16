using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Dao.WhatsCdc;

public static class Extensions
{
    internal static void CheckNull(this string source, string name)
    {
        if (string.IsNullOrWhiteSpace(source))
            throw new ArgumentNullException(name);
    }

    internal static void CheckNull(this object source, string name)
    {
        if (source == null)
            throw new ArgumentNullException(name);
    }

    internal static IEnumerable<T> ToEnumerable<T>(this T source)
    {
        yield return source;
    }

    internal static bool IsForAllInstances(this string captureInstance) =>
        string.IsNullOrWhiteSpace(captureInstance) || Consts.All.Equals(captureInstance, StringComparison.OrdinalIgnoreCase);

    public static bool IsNullEmptyOrZero(this byte[] source) => source == null || source.Length == 0 || source.All(a => a == 0);

    public static bool IsNullEmptyOrZero<T>(this T[] source)
        where T : struct, IComparable, IConvertible, IComparable<T>, IEquatable<T> =>
        source == null || source.Length == 0 || source.All(a => a.Equals(0));

    #region SqlHelper

    public static object AsNullable(this object o) => o == DBNull.Value ? null : o;
    public static object AsDBNull(this object o) => o ?? DBNull.Value;

    public static object GetValueNullable(this SqlDataReader reader, int ordinal) => reader.GetValue(ordinal).AsNullable();

    static TOut DefaultConvert<TIn, TOut>(TIn o) => o.AsNullable() == null ? default : (TOut)(object)o;

    public static async Task<T> ExecuteSqlCommandAsync<T>(this SqlConnection sqlConnection, string sql, Func<SqlCommand, Task<T>> executeSqlCommand, IEnumerable<SqlParameter> parameters, CommandType commandType = CommandType.Text, int commandTimeout = 30)
    {
        executeSqlCommand.CheckNull(nameof(executeSqlCommand));

        using var cmd = new SqlCommand(sql, sqlConnection);
        cmd.CommandType = commandType;
        cmd.CommandTimeout = commandTimeout;
        if (parameters != null)
        {
            foreach (var parameter in parameters)
            {
                parameter.Value = parameter.Value.AsDBNull();
                cmd.Parameters.Add(parameter);
            }
        }

        return await executeSqlCommand(cmd).ConfigureAwait(false);
    }

    public static async Task<List<T>> ToListAsync<T>(this SqlDataReader reader, Func<SqlDataReader, T> onReading)
    {
        onReading.CheckNull(nameof(onReading));

        var rows = new List<T>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            rows.Add(onReading(reader));
        }

        return rows;
    }

    public static async Task<List<IReadOnlyDictionary<string, object>>> ToDictionaryListAsync(this SqlDataReader reader) => await reader.ToListAsync(r =>
    {
        var row = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < r.FieldCount; i++)
        {
            //row.Add(reader.GetName(i), await reader.IsDBNullAsync(i).ConfigureAwait(false) ? null : reader.GetValue(i));
            row.Add(r.GetName(i), r.GetValueNullable(i));
        }

        return (IReadOnlyDictionary<string, object>)row;
    }).ConfigureAwait(false);

    public static async Task<List<KeyValuePair<TKey, TValue>>> ToKeyValueListAsync<TKey, TValue>(this SqlDataReader reader, Func<object, TKey> convertKey = null, Func<object, TValue> convertValue = null) => await reader.ToListAsync(r =>
    {
        var key = r.GetValueNullable(0);
        var value = r.GetValueNullable(1);
        return new KeyValuePair<TKey, TValue>(convertKey == null ? DefaultConvert<object, TKey>(key) : convertKey(key),
            convertValue == null ? DefaultConvert<object, TValue>(value) : convertValue(value));
    }).ConfigureAwait(false);

    public static async Task<List<T>> ToListAsync<T>(this SqlDataReader reader, Func<object, T> convertValue = null) => await reader.ToListAsync(r =>
    {
        var value = r.GetValueNullable(0);
        return convertValue == null ? DefaultConvert<object, T>(value) : convertValue(value);
    }).ConfigureAwait(false);

    public static async Task<T> ExecuteNonQueryAsync<T>(this SqlConnection sqlConnection, string sql, IEnumerable<SqlParameter> parameters = null, Func<int, T> generateReturnValue = null, CommandType commandType = CommandType.Text, int commandTimeout = 30)
    {
        sql.CheckNull(nameof(sql));

        return await sqlConnection.ExecuteSqlCommandAsync(sql, async cmd =>
        {
            var value = await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            return generateReturnValue == null ? DefaultConvert<int, T>(value) : generateReturnValue(value);
        }, parameters, commandType, commandTimeout).ConfigureAwait(false);
    }

    internal static async Task<T> SpExecuteNonQueryAsync<T>(this SqlConnection sqlConnection, string sql, IEnumerable<SqlParameter> parameters = null, Func<int, T> generateReturnValue = null, int commandTimeout = 30) =>
        await sqlConnection.ExecuteNonQueryAsync(sql, parameters, generateReturnValue, CommandType.StoredProcedure, commandTimeout).ConfigureAwait(false);

    public static async Task<T> ExecuteStoredProcedureWithReturnValueAsync<T>(this SqlConnection sqlConnection,
        string sql,
        SqlDbType sqlDbType,
        IEnumerable<SqlParameter> parameters = null,
        Func<object, T> convertValue = null,
        int commandTimeout = 30,
        string returnValueName = "@ReturnValue")
    {
        returnValueName.CheckNull(nameof(returnValueName));

        var parameterList = new List<SqlParameter>();
        if (parameters != null)
        {
            parameterList.AddRange(parameters);
        }

        var returnValue = new SqlParameter(returnValueName, sqlDbType) { Direction = ParameterDirection.ReturnValue };
        parameterList.Add(returnValue);

        return await sqlConnection.SpExecuteNonQueryAsync(sql, parameterList,
            _ => convertValue == null ? DefaultConvert<object, T>(returnValue.Value) : convertValue(returnValue.Value),
            commandTimeout).ConfigureAwait(false);
    }

    public static async Task<int> ExecuteStoredProcedureWithReturnValueIntAsync(this SqlConnection sqlConnection,
        string sql,
        IEnumerable<SqlParameter> parameters = null,
        int commandTimeout = 30,
        string returnValueName = "@ReturnValue") =>
        await sqlConnection.ExecuteStoredProcedureWithReturnValueAsync<int>(sql, SqlDbType.Int, parameters, null, commandTimeout, returnValueName).ConfigureAwait(false);

    public static async Task<T> ExecuteScalarAsync<T>(this SqlConnection sqlConnection, string sql, IEnumerable<SqlParameter> parameters = null, Func<object, T> generateReturnValue = null, CommandType commandType = CommandType.Text, int commandTimeout = 30)
    {
        sql.CheckNull(nameof(sql));

        return await sqlConnection.ExecuteSqlCommandAsync(sql, async cmd =>
        {
            var value = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
            return generateReturnValue == null ? DefaultConvert<object, T>(value) : generateReturnValue(value);
        }, parameters, commandType, commandTimeout).ConfigureAwait(false);
    }

    internal static async Task<T> SpExecuteScalarAsync<T>(this SqlConnection sqlConnection, string sql, IEnumerable<SqlParameter> parameters = null, Func<object, T> generateReturnValue = null, int commandTimeout = 30) =>
        await sqlConnection.ExecuteScalarAsync(sql, parameters, generateReturnValue, CommandType.StoredProcedure, commandTimeout).ConfigureAwait(false);

    public static async Task<T> ExecuteReaderAsync<T>(this SqlConnection sqlConnection, string sql, Func<SqlDataReader, Task<T>> generateReturnValue, IEnumerable<SqlParameter> parameters = null, CommandType commandType = CommandType.Text, int commandTimeout = 30)
    {
        sql.CheckNull(nameof(sql));
        generateReturnValue.CheckNull(nameof(generateReturnValue));

        return await sqlConnection.ExecuteSqlCommandAsync(sql, async cmd =>
        {
            using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            return await generateReturnValue(reader).ConfigureAwait(false);
        }, parameters, commandType, commandTimeout).ConfigureAwait(false);
    }

    internal static async Task<T> SpExecuteReaderAsync<T>(this SqlConnection sqlConnection, string sql, Func<SqlDataReader, Task<T>> generateReturnValue, IEnumerable<SqlParameter> parameters = null, int commandTimeout = 30) =>
        await ExecuteReaderAsync(sqlConnection, sql, generateReturnValue, parameters, CommandType.StoredProcedure, commandTimeout).ConfigureAwait(false);

    #endregion
}
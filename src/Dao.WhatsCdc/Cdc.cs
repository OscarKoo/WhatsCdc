using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Dao.WhatsCdc;

public class Cdc(SqlConnection sqlConnection, int commandTimeout = 30)
{
    protected internal readonly int commandTimeout = commandTimeout;
    protected internal readonly SqlConnection sqlConnection = sqlConnection;

    public async Task<int> EnableDbAsync()
    {
        var result = await this.Utility().HasDbCdcEnabledAsync().ConfigureAwait(false)
            ? default
            : await this.Api().EnableDbAsync().ConfigureAwait(false);

        await this.Utility().CreateFnsAsync().ConfigureAwait(false);
        return result;
    }

    public async Task<int> DisableDbAsync()
    {
        await this.Utility().DropFnsAsync().ConfigureAwait(false);

        return !await this.Utility().HasDbCdcEnabledAsync().ConfigureAwait(false)
            ? default
            : await this.Api().DisableDbAsync().ConfigureAwait(false);
    }

    public async Task<string> EnableTableAsync(string sourceName,
        string indexName = null,
        string capturedColumnList = null,
        string sourceSchema = "dbo",
        string captureInstance = null,
        bool supportsNetChanges = false,
        string roleName = null,
        string fileGroupName = null,
        bool allowPartitionSwitch = true)
    {
        sourceSchema.CheckNull(nameof(sourceSchema));

        if (string.IsNullOrWhiteSpace(captureInstance))
            captureInstance = $"{sourceSchema}_{sourceName}{(!supportsNetChanges ? null : "_Net")}";

        await EnableDbAsync().ConfigureAwait(false);

        if (!await this.Utility().HasTableCdcEnabledAsync($"{sourceSchema}.{sourceName}", captureInstance).ConfigureAwait(false))
            await this.Api().EnableTableAsync(sourceSchema,
                sourceName,
                captureInstance,
                supportsNetChanges,
                roleName,
                indexName,
                capturedColumnList,
                fileGroupName,
                allowPartitionSwitch).ConfigureAwait(false);

        return captureInstance;
    }

    public async Task<int> DisableTableAsync(string sourceName, string captureInstance = "all", string sourceSchema = "dbo") =>
        !await this.Utility().HasDbCdcEnabledAsync().ConfigureAwait(false)
        || !await this.Utility().HasTableCdcEnabledAsync(sourceName, captureInstance).ConfigureAwait(false)
            ? default
            : await this.Api().DisableTableAsync(sourceSchema, sourceName, captureInstance).ConfigureAwait(false);

    public async Task<List<string>> GetChangedColumnsAsync(string captureInstance, byte[] updateMask)
    {
        captureInstance.CheckNull(nameof(captureInstance));

        if (updateMask.IsNullEmptyOrZero())
            return [];

        await this.Utility().CreateFn_GetBit1FromHexAsync().ConfigureAwait(false);
        await this.Utility().CreateFn_GetChangedColumnsAsync().ConfigureAwait(false);

        const string sql = $"SELECT * FROM dbo.{Consts.Fn_GetChangedColumns}(@CaptureInstance, @UpdateMask)";
        return await this.sqlConnection.ExecuteReaderAsync(sql, async reader => await reader.ToListAsync<string>().ConfigureAwait(false), new SqlParameter[]
        {
            new("@CaptureInstance", captureInstance),
            new("@UpdateMask", updateMask)
        }, commandTimeout: this.commandTimeout).ConfigureAwait(false) ?? [];
    }

    public async Task<List<string>> GetBinaryColumnsAsync(string captureInstance)
    {
        captureInstance.CheckNull(nameof(captureInstance));

        await this.Utility().CreateFn_GetBinaryColumnsAsync().ConfigureAwait(false);

        const string sql = $"SELECT * FROM dbo.{Consts.Fn_GetBinaryColumns}(@CaptureInstance)";
        return await this.sqlConnection.ExecuteReaderAsync(sql, async reader => await reader.ToListAsync<string>().ConfigureAwait(false), new SqlParameter[]
        {
            new("@CaptureInstance", captureInstance)
        }, commandTimeout: this.commandTimeout).ConfigureAwait(false);
    }

    public async Task<IReadOnlyDictionary<string, ChangeTable[]>> GetCaptureInstancesAsync(string sourceName = null)
    {
        var sql = $@"
SELECT o.name, t.capture_instance, t.supports_net_changes
FROM cdc.change_tables t
JOIN sys.objects o ON o.object_id = t.source_object_id
WHERE o.type = 'U'
    {(string.IsNullOrWhiteSpace(sourceName) ? null : "AND t.name = @SourceName")}
";
        var parameters = string.IsNullOrWhiteSpace(sourceName)
            ? null
            : new SqlParameter[] { new("@SourceName", sourceName) };

        var result = await this.sqlConnection.ExecuteReaderAsync(sql,
            async reader => await reader.ToListAsync(r => new ChangeTable((string)r.GetValueNullable(0), (string)r.GetValueNullable(1), (bool)r.GetValueNullable(2))).ConfigureAwait(false),
            parameters, commandTimeout: this.commandTimeout).ConfigureAwait(false);

        return result.GroupBy(g => g.SourceName, StringComparer.OrdinalIgnoreCase).ToDictionary(k => k.Key, v => v.Select(s => s).ToArray(), StringComparer.OrdinalIgnoreCase);
    }

    public async Task<List<string>> GetSourceNamesAsync(string captureInstance = null)
    {
        var sql = $@"
SELECT o.name
FROM cdc.change_tables t
JOIN sys.objects o ON o.object_id = t.source_object_id
WHERE o.type = 'U'
    {(string.IsNullOrWhiteSpace(captureInstance) ? null : "AND t.capture_instance = @CaptureInstance")}
";
        var parameters = string.IsNullOrWhiteSpace(captureInstance)
            ? null
            : new SqlParameter[] { new("@CaptureInstance", captureInstance) };

        var result = await this.sqlConnection.ExecuteReaderAsync(sql, async reader => await reader.ToListAsync<string>().ConfigureAwait(false), parameters, commandTimeout: this.commandTimeout).ConfigureAwait(false);

        return result.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    async Task<ChangedSet> GetChangesAsync(string captureInstance, bool isNet, StringEnum defaultRowFilterOption, byte[] fromLsn = null, StringEnum rowFilterOption = null)
    {
        captureInstance.CheckNull(captureInstance);

        var result = await ChangedSet.CreateAsync(this, captureInstance).ConfigureAwait(false);
        result.IsNet = isNet;

        if (fromLsn.IsNullEmptyOrZero())
        {
            fromLsn = await this.Api().GetMinLsnAsync(captureInstance).ConfigureAwait(false);
            if (fromLsn.IsNullEmptyOrZero())
                return result;
        }

        rowFilterOption ??= defaultRowFilterOption;

        var cache = new Dictionary<byte[], List<string>>();
        var hasRetried = isNet;
        Retry:
        try
        {
            result.Rows = [];
            var lastLsn = await this.Api().GetMaxLsnAsync().ConfigureAwait(false);
            if (lastLsn.IsNullEmptyOrZero())
                return result;

            var list = await this.Api().GetChangesAsync(!isNet ? "all" : "net", captureInstance, fromLsn, lastLsn, rowFilterOption).ConfigureAwait(false);
            foreach (var item in list)
            {
                result.Rows.Add(await item.ToChangedRowAsync(this, captureInstance, cache).ConfigureAwait(false));
            }

            if (!isNet)
                result.Rows.MergeChangesAsync();
            result.LastLsn = lastLsn;
            return result;
        }
        catch (CdcRequireRetryException ex)
        {
            if (hasRetried)
                throw;

            hasRetried = true;
            goto Retry;
        }
    }

    public async Task<ChangedSet> GetAllChangesAsync(string captureInstance, byte[] fromLsn = null, AllChangesRowFilterOption rowFilterOption = null) =>
        await GetChangesAsync(captureInstance, false, AllChangesRowFilterOption.AllUpdateOld, fromLsn, rowFilterOption).ConfigureAwait(false);

    public async Task<ChangedSet> GetNetChangesAsync(string captureInstance, byte[] fromLsn = null, NetChangesRowFilterOption rowFilterOption = null) =>
        await GetChangesAsync(captureInstance, true, NetChangesRowFilterOption.AllWithMask, fromLsn, rowFilterOption).ConfigureAwait(false);
}

public static class CdcExtensions
{
    public static Cdc Cdc(this SqlConnection sqlConnection, int commandTimeout = 30) => new(sqlConnection, commandTimeout);
}
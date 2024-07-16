using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Dao.WhatsCdc;

public class CdcApi(SqlConnection sqlConnection, int commandTimeout = 30)
{
    #region fn

    #region changes

    internal async Task<List<IReadOnlyDictionary<string, object>>> GetChangesAsync(string type, string captureInstance, byte[] fromLsn, byte[] toLsn, StringEnum rowFilterOption)
    {
        captureInstance.CheckNull(nameof(captureInstance));
        fromLsn.CheckNull(nameof(fromLsn));
        toLsn.CheckNull(nameof(toLsn));
        rowFilterOption.CheckNull(nameof(rowFilterOption));

        return await sqlConnection.ExecuteReaderAsync($"SELECT *, sys.fn_cdc_map_lsn_to_time([__$start_lsn]) AS [{CdcColumnName.UpdateTime}] FROM cdc.fn_cdc_get_{type}_changes_{captureInstance}(@from_lsn, @to_lsn, @row_filter_option)", async reader => await reader.ToDictionaryListAsync().ConfigureAwait(false), new SqlParameter[]
        {
            new("@from_lsn", fromLsn) { SqlDbType = SqlDbType.Binary },
            new("@to_lsn", toLsn) { SqlDbType = SqlDbType.Binary },
            new("@row_filter_option", rowFilterOption.Value)
        }, commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    public async Task<List<IReadOnlyDictionary<string, object>>> GetAllChangesAsync(string captureInstance, byte[] fromLsn, byte[] toLsn, AllChangesRowFilterOption rowFilterOption) =>
        await GetChangesAsync("all", captureInstance, fromLsn, toLsn, rowFilterOption).ConfigureAwait(false);

    public async Task<List<IReadOnlyDictionary<string, object>>> GetNetChangesAsync(string captureInstance, byte[] fromLsn, byte[] toLsn, NetChangesRowFilterOption rowFilterOption) =>
        await GetChangesAsync("net", captureInstance, fromLsn, toLsn, rowFilterOption).ConfigureAwait(false);

    #endregion

    #region lsn

    async Task<byte[]> LsnAsync(string type, byte[] lsnValue) =>
        await sqlConnection.ExecuteScalarAsync<byte[]>($"SELECT sys.fn_cdc_{type}_lsn(@lsn_value)", new SqlParameter[]
        {
            new("@lsn_value", lsnValue) { SqlDbType = SqlDbType.Binary }
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    public async Task<byte[]> DecrementLsnAsync(byte[] lsnValue) => await LsnAsync("decrement", lsnValue).ConfigureAwait(false);

    public async Task<byte[]> IncrementLsnAsync(byte[] lsnValue) => await LsnAsync("increment", lsnValue).ConfigureAwait(false);

    public async Task<byte[]> GetMaxLsnAsync() =>
        await sqlConnection.ExecuteScalarAsync<byte[]>("SELECT sys.fn_cdc_get_max_lsn()", commandTimeout: commandTimeout).ConfigureAwait(false);

    public async Task<byte[]> GetMinLsnAsync(string captureInstance) =>
        await sqlConnection.ExecuteScalarAsync<byte[]>("SELECT sys.fn_cdc_get_min_lsn(@capture_instance)", new SqlParameter[]
        {
            new("@capture_instance", captureInstance)
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    public async Task<DateTime?> MapLsnToTimeAsync(byte[] lsnValue) =>
        await sqlConnection.ExecuteScalarAsync<DateTime?>("SELECT sys.fn_cdc_map_lsn_to_time(@lsn_value)", new SqlParameter[]
        {
            new("@lsn_value", lsnValue) { SqlDbType = SqlDbType.Binary }
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    public async Task<byte[]> MapTimeToLsnAsync(RelationalOperator relationalOperator, DateTime? trackingTime)
    {
        relationalOperator.CheckNull(nameof(relationalOperator));

        return await sqlConnection.ExecuteScalarAsync<byte[]>("SELECT sys.fn_cdc_map_time_to_lsn(@relational_operator, @tracking_time)", new SqlParameter[]
        {
            new("@relational_operator", relationalOperator.Value),
            new("@tracking_time", trackingTime)
        }, commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    #endregion

    #region others

    public async Task<int?> GetColumnOrdinalAsync(string captureInstance, string columnName) =>
        await sqlConnection.ExecuteScalarAsync<int?>("SELECT sys.fn_cdc_get_column_ordinal(@capture_instance, @column_name)", new SqlParameter[]
        {
            new("@capture_instance", captureInstance),
            new("@column_name", columnName)
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    public async Task<bool?> HasColumnChangedAsync(string captureInstance, string columnName, byte[] updateMask) =>
        await sqlConnection.ExecuteScalarAsync<bool?>("SELECT sys.fn_cdc_has_column_changed(@capture_instance, @column_name, @update_mask)", new SqlParameter[]
        {
            new("@capture_instance", captureInstance),
            new("@column_name", columnName),
            new("@update_mask", updateMask) { SqlDbType = SqlDbType.VarBinary }
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    public async Task<bool?> IsBitSetAsync(int position, byte[] updateMask) =>
        await sqlConnection.ExecuteScalarAsync<bool?>("SELECT sys.fn_cdc_is_bit_set(@position, @update_mask)", new SqlParameter[]
        {
            new("@position", position),
            new("@update_mask", updateMask) { SqlDbType = SqlDbType.VarBinary }
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    #endregion

    #endregion

    #region sp

    #region job

    async Task<int> JobAsync(string sql, JobType jobType, IEnumerable<SqlParameter> parameters = null)
    {
        jobType.CheckNull(nameof(jobType));

        var parameterList = new List<SqlParameter> { new("@job_type", jobType.Value) };
        if (parameters != null)
            parameterList.AddRange(parameters);

        return await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync(sql, parameterList, commandTimeout).ConfigureAwait(false);
    }

    public async Task<int> AddJobAsync(JobType jobType,
        bool startJob = true,
        int maxTrans = 500,
        int maxScans = 10,
        bool continuous = true,
        long pollingInterval = 5,
        long retention = 4320,
        long threshold = 5000,
        bool checkForLogReader = false) =>
        await JobAsync("sys.sp_cdc_add_job", jobType, new SqlParameter[]
        {
            new("@start_job", startJob),
            new("@maxtrans", maxTrans),
            new("@maxscans", maxScans),
            new("@continuous", continuous),
            new("@pollinginterval", pollingInterval),
            new("@retention", retention),
            new("@threshold", threshold),
            new("@check_for_logreader", checkForLogReader)
        }).ConfigureAwait(false);

    public async Task<int> ChangeJobAsync(JobType jobType,
        int? maxTrans = null,
        int? maxScans = null,
        bool? continuous = null,
        long? pollingInterval = null,
        long? retention = null,
        long? threshold = null) =>
        await JobAsync("sys.sp_cdc_change_job", jobType, new SqlParameter[]
        {
            new("@maxtrans", maxTrans),
            new("@maxscans", maxScans),
            new("@continuous", continuous),
            new("@pollinginterval", pollingInterval),
            new("@retention", retention),
            new("@threshold", threshold)
        }).ConfigureAwait(false);

    public async Task<int> DropJobAsync(JobType jobType) => await JobAsync("sys.sp_cdc_drop_job", jobType).ConfigureAwait(false);

    public async Task<List<IReadOnlyDictionary<string, object>>> HelpJobsAsync() =>
        await sqlConnection.SpExecuteReaderAsync("sys.sp_cdc_help_jobs", async reader => await reader.ToDictionaryListAsync().ConfigureAwait(false), null, commandTimeout).ConfigureAwait(false);

    public async Task<int> StartJobAsync(JobType jobType) => await JobAsync("sys.sp_cdc_start_job", jobType).ConfigureAwait(false);

    public async Task<int> StopJobAsync(JobType jobType) => await JobAsync("sys.sp_cdc_stop_job", jobType).ConfigureAwait(false);

    #endregion

    #region db

    public async Task<int> EnableDbAsync() =>
        await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync("sys.sp_cdc_enable_db", null, commandTimeout).ConfigureAwait(false);

    public async Task<int> DisableDbAsync() =>
        await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync("sys.sp_cdc_disable_db", null, commandTimeout).ConfigureAwait(false);

    #endregion

    #region table

    public async Task<int> EnableTableAsync(string sourceSchema,
        string sourceName,
        string captureInstance = null,
        bool supportsNetChanges = false,
        string roleName = null,
        string indexName = null,
        string capturedColumnList = null,
        string fileGroupName = null,
        bool allowPartitionSwitch = true)
    {
        sourceSchema.CheckNull(nameof(sourceSchema));
        sourceName.CheckNull(nameof(sourceName));

        return await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync("sys.sp_cdc_enable_table", new SqlParameter[]
        {
            new("@source_schema", sourceSchema),
            new("@source_name", sourceName),
            new("@capture_instance", captureInstance),
            new("@supports_net_changes", supportsNetChanges),
            new("@role_name", roleName),
            new("@index_name", indexName),
            new("@captured_column_list", capturedColumnList),
            new("@filegroup_name", fileGroupName),
            new("@allow_partition_switch", allowPartitionSwitch)
        }, commandTimeout).ConfigureAwait(false);
    }

    public async Task<int> DisableTableAsync(string sourceSchema, string sourceName, string captureInstance = "all")
    {
        sourceSchema.CheckNull(nameof(sourceSchema));
        sourceName.CheckNull(nameof(sourceName));
        captureInstance.CheckNull(nameof(captureInstance));

        return await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync("sys.sp_cdc_disable_table", new SqlParameter[]
        {
            new("@source_schema", sourceSchema),
            new("@source_name", sourceName),
            new("@capture_instance", captureInstance)
        }, commandTimeout).ConfigureAwait(false);
    }

    public async Task<int> CleanupChangeTableAsync(string captureInstance, byte[] lowWaterMark = null, long threshold = 5000) =>
        await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync("sys.sp_cdc_cleanup_change_table", new SqlParameter[]
        {
            new("@capture_instance", captureInstance),
            new("@low_water_mark", lowWaterMark) { SqlDbType = SqlDbType.Binary },
            new("@threshold", threshold)
        }, commandTimeout).ConfigureAwait(false);

    #endregion

    #region others

    public async Task<List<IReadOnlyDictionary<string, object>>> GenerateWrapperFunctionAsync(string captureInstance = null, bool closedHighEndPoint = true, string columnList = null, string updateFlagList = null) =>
        await sqlConnection.SpExecuteReaderAsync("sys.sp_cdc_generate_wrapper_function", async reader => await reader.ToDictionaryListAsync().ConfigureAwait(false), new SqlParameter[]
        {
            new("@capture_instance", captureInstance),
            new("@closed_high_end_point", closedHighEndPoint),
            new("@column_list", columnList),
            new("@update_flag_list", updateFlagList)
        }, commandTimeout).ConfigureAwait(false);

    public async Task<List<IReadOnlyDictionary<string, object>>> GetCapturedColumnsAsync(string captureInstance)
    {
        captureInstance.CheckNull(nameof(captureInstance));

        return await sqlConnection.SpExecuteReaderAsync("sys.sp_cdc_get_captured_columns", async reader => await reader.ToDictionaryListAsync().ConfigureAwait(false), new SqlParameter[]
        {
            new("@capture_instance", captureInstance)
        }, commandTimeout).ConfigureAwait(false);
    }

    public async Task<List<IReadOnlyDictionary<string, object>>> GetDdlHistoryAsync(string captureInstance)
    {
        captureInstance.CheckNull(nameof(captureInstance));

        return await sqlConnection.SpExecuteReaderAsync("sys.sp_cdc_get_ddl_history", async reader => await reader.ToDictionaryListAsync().ConfigureAwait(false), new SqlParameter[]
        {
            new("@capture_instance", captureInstance)
        }, commandTimeout).ConfigureAwait(false);
    }

    public async Task<List<IReadOnlyDictionary<string, object>>> HelpChangeDataCaptureAsync(string sourceSchema = null, string sourceName = null) =>
        await sqlConnection.SpExecuteReaderAsync("sys.sp_cdc_help_change_data_capture", async reader => await reader.ToDictionaryListAsync().ConfigureAwait(false), new SqlParameter[]
        {
            new("@source_schema", sourceSchema),
            new("@source_name", sourceName)
        }, commandTimeout).ConfigureAwait(false);

    public async Task<int> ScanAsync(int maxTrans = 500, int maxScans = 10, byte continuous = 0, long pollingInterval = 0, int isFromJob = 0) =>
        await sqlConnection.ExecuteStoredProcedureWithReturnValueIntAsync("sys.sp_cdc_scan", new SqlParameter[]
        {
            new("@maxtrans", maxTrans),
            new("@maxscans", maxScans),
            new("@continuous", continuous),
            new("@pollinginterval", pollingInterval),
            new("@is_from_job", isFromJob)
        }, commandTimeout).ConfigureAwait(false);

    #endregion

    #endregion
}

public static class CdcApiExtensions
{
    public static CdcApi Api(this Cdc cdc) => new(cdc.sqlConnection, cdc.commandTimeout);
}
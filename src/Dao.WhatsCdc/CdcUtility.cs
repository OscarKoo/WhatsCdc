using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.IdentityModel.Tokens;

namespace Dao.WhatsCdc;

internal class CdcUtility(SqlConnection sqlConnection, int commandTimeout = 30)
{
    internal async Task<int?> ObjectIdAsync(string objectName, string objectType) =>
        await sqlConnection.ExecuteScalarAsync<int?>("SELECT OBJECT_ID(@object_name, @object_type)", new SqlParameter[]
        {
            new("@object_name", objectName),
            new("@object_type", objectType)
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

    #region fn

    internal async Task CreateFn_GetBit1FromHexAsync()
    {
        if (await ObjectIdAsync($"dbo.{Consts.Fn_GetBit1FromHex}", "TF").ConfigureAwait(false) != null)
            return;

        const string sql = $@"
CREATE FUNCTION dbo.{Consts.Fn_GetBit1FromHex}
(
    @HexValue VARBINARY(128)
)
RETURNS
@Result TABLE
(
    Position BIGINT
)
AS
BEGIN
    IF @HexValue IS NULL
        RETURN
    
    DECLARE @IntValue BIGINT = CAST(@HexValue AS BIGINT)

    ;WITH cteBinary AS (
        SELECT CONVERT(BIGINT, 0) AS RowIndex, @IntValue AS Number, CONVERT(BIGINT, NULL) AS BitValue
        UNION ALL
        SELECT RowIndex + 1, Number / 2, Number % 2
        FROM cteBinary
        WHERE Number > 0
    )
    INSERT INTO @Result (Position)
    SELECT c.RowIndex FROM cteBinary c WHERE c.BitValue > 0

    RETURN 
END";

        await sqlConnection.ExecuteNonQueryAsync<int>(sql, commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    async Task DropFn_GetBit1FromHexAsync()
    {
        if (await ObjectIdAsync($"dbo.{Consts.Fn_GetBit1FromHex}", "TF").ConfigureAwait(false) == null)
            return;

        await sqlConnection.ExecuteNonQueryAsync<int>($"DROP FUNCTION dbo.{Consts.Fn_GetBit1FromHex}", commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    internal async Task CreateFn_GetChangedColumnsAsync()
    {
        if (await ObjectIdAsync($"dbo.{Consts.Fn_GetChangedColumns}", "IF").ConfigureAwait(false) != null)
            return;

        const string sql = $@"
CREATE FUNCTION dbo.{Consts.Fn_GetChangedColumns}
(    
    @CaptureInstance NVARCHAR(128),
    @UpdateMask VARBINARY(128)
)
RETURNS TABLE
AS
RETURN
(
    SELECT cc.column_name
    FROM cdc.captured_columns cc
    CROSS APPLY (
        SELECT t.source_object_id FROM cdc.change_tables t WHERE t.capture_instance = @CaptureInstance AND t.object_id = cc.object_id
    ) ct
    WHERE EXISTS (SELECT * FROM dbo.{Consts.Fn_GetBit1FromHex}(@UpdateMask) b WHERE b.Position = cc.column_ordinal)
        AND EXISTS (
            SELECT *
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE c.object_id = ct.source_object_id
            AND c.name = cc.column_name
            AND c.is_identity = 0
            AND t.name != 'timestamp'
        )
)";

        await sqlConnection.ExecuteNonQueryAsync<int>(sql, commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    async Task DropFn_GetChangedColumnsAsync()
    {
        if (await ObjectIdAsync($"dbo.{Consts.Fn_GetChangedColumns}", "IF").ConfigureAwait(false) == null)
            return;

        await sqlConnection.ExecuteNonQueryAsync<int>($"DROP FUNCTION dbo.{Consts.Fn_GetChangedColumns}", commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    internal async Task CreateFn_GetBinaryColumnsAsync()
    {
        if (await ObjectIdAsync($"dbo.{Consts.Fn_GetBinaryColumns}", "IF").ConfigureAwait(false) != null)
            return;

        const string sql = $@"
CREATE FUNCTION dbo.{Consts.Fn_GetBinaryColumns}
(    
    @CaptureInstance NVARCHAR(128)
)
RETURNS TABLE
AS
RETURN
(
    SELECT cc.column_name
    FROM cdc.captured_columns cc
    WHERE EXISTS (
        SELECT *
        FROM cdc.change_tables ct
        JOIN sys.columns c ON c.object_id = ct.source_object_id
        JOIN sys.types t ON t.user_type_id = c.user_type_id
        WHERE ct.object_id = cc.object_id
            AND ct.capture_instance = @CaptureInstance
            AND c.name = cc.column_name
            AND t.name IN ('image', 'varbinary', 'binary')
    )
)";

        await sqlConnection.ExecuteNonQueryAsync<int>(sql, commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    async Task DropFn_GetBinaryColumnsAsync()
    {
        if (await ObjectIdAsync($"dbo.{Consts.Fn_GetBinaryColumns}", "IF").ConfigureAwait(false) == null)
            return;

        await sqlConnection.ExecuteNonQueryAsync<int>($"DROP FUNCTION dbo.{Consts.Fn_GetBinaryColumns}", commandTimeout: commandTimeout).ConfigureAwait(false);
    }

    internal async Task CreateFnsAsync()
    {
        await CreateFn_GetBit1FromHexAsync().ConfigureAwait(false);
        await CreateFn_GetChangedColumnsAsync().ConfigureAwait(false);
        await CreateFn_GetBinaryColumnsAsync().ConfigureAwait(false);
    }

    internal async Task DropFnsAsync()
    {
        await DropFn_GetChangedColumnsAsync().ConfigureAwait(false);
        await DropFn_GetBit1FromHexAsync().ConfigureAwait(false);
        await DropFn_GetBinaryColumnsAsync().ConfigureAwait(false);
    }

    #endregion

    #region hasEnabled

    internal async Task<bool> HasDbCdcEnabledAsync() =>
        await sqlConnection.ExecuteScalarAsync<bool?>("SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()", commandTimeout: commandTimeout).ConfigureAwait(false) ?? false;

    internal async Task<bool> HasTableCdcEnabledAsync(string sourceName, string captureInstance = null)
    {
        sourceName.CheckNull(nameof(sourceName));

        const string sql = "SELECT capture_instance FROM cdc.change_tables WHERE source_object_id = OBJECT_ID(@object_name, 'U')";
        var instances = await sqlConnection.ExecuteReaderAsync(sql, async reader => await reader.ToListAsync<string>().ConfigureAwait(false), new SqlParameter[]
        {
            new("@object_name", sourceName)
        }, commandTimeout: commandTimeout).ConfigureAwait(false);

        return !instances.IsNullOrEmpty()
            && (instances.Count > 1
                || captureInstance.IsForAllInstances()
                || instances[0].Equals(captureInstance, StringComparison.OrdinalIgnoreCase));
    }

    #endregion
}

internal static class CdcUtilityExtensions
{
    internal static CdcUtility Utility(this Cdc cdc) => new(cdc.sqlConnection, cdc.commandTimeout);
}
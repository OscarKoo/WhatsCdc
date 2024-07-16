using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;

namespace Dao.WhatsCdc;

public class ChangedRow
{
    public byte[] StartLsn { get; set; }
    public byte[] SeqVal { get; set; }
    public CdcOperation Operation { get; set; }
    public byte[] UpdateMask { get; set; }
    public DateTime UpdateTime { get; set; }

    public Dictionary<string, ChangedColumnValue> ChangedColumns { get; set; }
    public bool ChangedColumnsMerged { get; set; }

    public Dictionary<string, object> Row { get; set; }
}

public class ChangedSet
{
    public string SourceName { get; set; }
    public string CaptureInstance { get; set; }
    public byte[] LastLsn { get; set; }
    public bool IsNet { get; set; }

    public List<string> BinaryColumns { get; set; }
    public List<ChangedRow> Rows { get; set; }

    internal static async Task<ChangedSet> CreateAsync(Cdc cdc, string captureInstance) =>
        new()
        {
            CaptureInstance = captureInstance,
            SourceName = (await cdc.GetSourceNamesAsync(captureInstance).ConfigureAwait(false)).FirstOrDefault(),
            BinaryColumns = await cdc.GetBinaryColumnsAsync(captureInstance).ConfigureAwait(false)
        };
}

public class ChangedColumnValue
{
    public object OldValue { get; set; }
    public object NewValue { get; set; }
    public bool HasOldValue { get; set; }
}

internal static class ChangedRowExtensions
{
    static readonly Dictionary<string, Action<KeyValuePair<string, object>, ChangedRow>> mappings = new(StringComparer.OrdinalIgnoreCase)
    {
        { CdcColumnName.StartLsn, (kv, row) => row.StartLsn = (byte[])kv.Value.AsNullable() },
        { CdcColumnName.SeqVal, (kv, row) => row.SeqVal = (byte[])kv.Value.AsNullable() },
        { CdcColumnName.Operation, (kv, row) => row.Operation = (CdcOperation)(int)kv.Value.AsNullable() },
        { CdcColumnName.UpdateMask, (kv, row) => row.UpdateMask = (byte[])kv.Value.AsNullable() },
        { CdcColumnName.UpdateTime, (kv, row) => row.UpdateTime = (DateTime)kv.Value.AsNullable() }
    };

    internal static async Task<ChangedRow> ToChangedRowAsync(this IReadOnlyDictionary<string, object> source,
        Cdc cdc = null, string captureInstance = null, Dictionary<byte[], List<string>> cache = null)
    {
        if (source == null)
            return null;

        var row = new ChangedRow
        {
            Row = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
        };

        foreach (var kv in source)
        {
            if (mappings.TryGetValue(kv.Key, out var mapping))
            {
                mapping(kv, row);
            }
            else
            {
                row.Row[kv.Key] = kv.Value;
            }
        }

        await row.RetrieveChangedColumnsAsync(cdc, captureInstance, cache).ConfigureAwait(false);

        return row;
    }

    static async Task RetrieveChangedColumnsAsync(this ChangedRow row, Cdc cdc, string captureInstance, IDictionary<byte[], List<string>> cache)
    {
        if (cdc == null || string.IsNullOrWhiteSpace(captureInstance) || cache == null
            || row.UpdateMask.IsNullEmptyOrZero() || row.Operation is not (CdcOperation.UpdateOld or CdcOperation.Update))
            return;

        if (!cache.TryGetValue(row.UpdateMask, out var columns))
            cache[row.UpdateMask] = columns = await cdc.GetChangedColumnsAsync(captureInstance, row.UpdateMask).ConfigureAwait(false);

        row.ChangedColumns = columns.ToDictionary(k => k, v =>
        {
            var col = new ChangedColumnValue();
            if (row.Operation == CdcOperation.UpdateOld)
            {
                col.OldValue = row.Row.GetValueOrDefault(v);
                col.HasOldValue = true;
            }
            else
            {
                col.NewValue = row.Row.GetValueOrDefault(v);
            }

            return col;
        }, StringComparer.OrdinalIgnoreCase);
    }

    internal static void MergeChangesAsync(this List<ChangedRow> rows)
    {
        if (rows.IsNullOrEmpty())
            return;

        var rowBeforeUpdate = -1;
        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            if (row.ChangedColumnsMerged
                || row.Operation is not CdcOperation.UpdateOld and not CdcOperation.Update
                || row.UpdateMask.IsNullEmptyOrZero())
                continue;

            if (row.ChangedColumns.IsNullOrEmpty())
                throw new CdcRequireRetrieveChangedColumnsException("Should retrieve the changed columns first, and then do the merge.");

            // ready to merge
            if (rowBeforeUpdate >= 0)
            {
                if (i != rowBeforeUpdate + 1 || row.Operation != CdcOperation.Update)
                    throw new CdcWithoutCorrespondingOperationException($"The previous operation ({CdcOperation.UpdateOld}) row index is ({rowBeforeUpdate}), and the current row index is ({i}) and operation is ({row.Operation}).");

                var previousRow = rows[rowBeforeUpdate];

                foreach (var p in previousRow.ChangedColumns)
                {
                    if (!row.ChangedColumns.TryGetValue(p.Key, out var c))
                        continue;

                    c.OldValue = p.Value.OldValue;
                    c.HasOldValue = p.Value.HasOldValue;
                }

                row.ChangedColumnsMerged = true;
                rows.RemoveAt(rowBeforeUpdate);
                rowBeforeUpdate = -1;
                i--;
                continue;
            }

            // detect UpdateOld
            if (row.Operation == CdcOperation.UpdateOld)
                rowBeforeUpdate = i;
            //// reset Update without previous UpdateOld
            //else
            //    row.ChangedColumns = null;
        }

        // only UpdateOld without Update
        if (rowBeforeUpdate >= 0)
            throw new CdcWithoutCorrespondingOperationException($"The operation ({CdcOperation.UpdateOld}) row ({rowBeforeUpdate}) without the corresponding operation ({CdcOperation.Update})");
    }
}
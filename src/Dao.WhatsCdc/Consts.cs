namespace Dao.WhatsCdc;

internal class Consts
{
    internal const string All = "all";
    internal const string Fn_GetBit1FromHex = "fn_GetBit1FromHex";
    internal const string Fn_GetChangedColumns = "fn_cdc_GetChangedColumns";
    internal const string Fn_GetBinaryColumns = "fn_cdc_GetBinaryColumns";
}

internal class CdcColumnName
{
    public const string StartLsn = "__$start_lsn";
    public const string SeqVal = "__$seqval";
    public const string Operation = "__$operation";
    public const string UpdateMask = "__$update_mask";
    public const string UpdateTime = "__$UpdateTime";
}
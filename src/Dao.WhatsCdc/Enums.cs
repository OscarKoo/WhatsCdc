namespace Dao.WhatsCdc;

public abstract class StringEnum
{
    protected internal StringEnum(string value) => Value = value;

    public string Value { get; }
    public override string ToString() => Value;
}

public class JobType : StringEnum
{
    public static readonly JobType Capture = new("capture");
    public static readonly JobType Cleanup = new("cleanup");

    internal JobType(string value) : base(value)
    {
    }
}

public class AllChangesRowFilterOption : StringEnum
{
    public static readonly AllChangesRowFilterOption All = new("all");
    public static readonly AllChangesRowFilterOption AllUpdateOld = new("all update old");

    internal AllChangesRowFilterOption(string value) : base(value)
    {
    }
}

public class NetChangesRowFilterOption : StringEnum
{
    public static readonly NetChangesRowFilterOption All = new("all");
    public static readonly NetChangesRowFilterOption AllWithMask = new("all with mask");
    public static readonly NetChangesRowFilterOption AllWithMerge = new("all with merge");

    internal NetChangesRowFilterOption(string value) : base(value)
    {
    }
}

public class RelationalOperator : StringEnum
{
    public static readonly RelationalOperator LargestLessThan = new("largest less than");
    public static readonly RelationalOperator LargestLessThanOrEqual = new("largest less than or equal");
    public static readonly RelationalOperator SmallestGreaterThan = new("smallest greater than");
    public static readonly RelationalOperator SmallestGreaterThanOrEqual = new("smallest greater than or equal");

    internal RelationalOperator(string value) : base(value)
    {
    }
}

public enum CdcOperation
{
    None = 0,
    Delete = 1,
    Insert = 2,
    UpdateOld = 3,
    Update = 4,
    InsertOrUpdate = 5
}
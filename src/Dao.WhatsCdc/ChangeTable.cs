namespace Dao.WhatsCdc;

public class ChangeTable(string sourceName = default, string captureInstance = default, bool supportsNetChanges = default)
{
    public string SourceName { get; set; } = sourceName;
    public string CaptureInstance { get; set; } = captureInstance;
    public bool SupportsNetChanges { get; set; } = supportsNetChanges;
}
using System;

namespace Dao.WhatsCdc;

public class CdcException(string message = null, Exception innerException = null) : Exception(message, innerException);

public class CdcRequireRetrieveChangedColumnsException(string message = null, Exception innerException = null) : CdcException(message, innerException);


public abstract class CdcRequireRetryException(string message = null, Exception innerException = null) : CdcException(message, innerException);

public class CdcWithoutCorrespondingOperationException(string message = null, Exception innerException = null) : CdcRequireRetryException(message, innerException);
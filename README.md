# Full Implementation of SQLServer Change Data Capture (CDC) API

Install the [NuGet package](https://www.nuget.org/packages/Dao.WhatsCdc).

## How To Use
### Some commonly used methods
#### Use ".Cdc()" to access the method
These methods are enhanced, and they will also call CdcApi internally.
```c#
using var connection = new SqlConnection("connectionString");
await connection.OpenAsync().ConfigureAwait(false);
var result = await connection.Cdc().XXXXXX().ConfigureAwait(false);
```

### Access the full implementation of CDC API
#### Use ".Cdc().Api()" to access the api
If you don't like the above commonly used methods and want the full control of CDC API.
```c#
using var connection = new SqlConnection("connectionString");
await connection.OpenAsync().ConfigureAwait(false);
var result = await connection.Cdc().Api().XXXXXX().ConfigureAwait(false);
```
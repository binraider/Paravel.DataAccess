# Paravel.DataAccess
Simple Data access library using Microsoft.Data.SqlClient 5.2.2
Its another variant of the MHP20ClassLib which i thought that i had put behind me after EF core and Dapper. 
However I still have need of something even more lightweight.

```
It uses a section in the appsettings like so"

 "ParavelDataAccess": {
    "ConnectionString": "Server=<SERVER>;Database=<DATABASE>;User ID=<USERID>;Password=<PASSWORD>",
    "CommandTimeout":  20
  }
```
# Spring Boot - ArchLens 

Spring Boot project to connect t0 external table and extract  binary data and View or Download it into the original file.

## APIs

This project has 5 APIs:
1.```/ArchLens``` - To Test Application is up and running or not.
2. ```/data-sources ``` - To get the list of all the data sources in property.json.
3. ``` /schemas?dataSource= ``` - To get the list schema present in the selected data source, that accepts 1 query parameter dataSource.
4. ``` /tables?dataSource=dataSourceName&schema=schemaName ``` - To get the list tables present in the selected schema, 
that accepts 2 query parameters dataSource and schema.

5. ``` /view?dataSource=dataSourceName&schema=schemaName&table=tableName&blobColName=colName&fileName=colName&idName=IdColName&idVal=val``` -
To view the Blob Content that presents in the table
This API accepts 7 query parameters:
  1. dataSource = Data source name.
  2. schema = Schema/Database name.
  3. table = table name.
  4. blobColName = Column name of where the Blob data is present.
  5. fileName = Column name of the File name that is present in the table.
  6. idName = Column name of the ID or Unique & Not Null column name.
  7. idVal = Value of idName Column name.

6.  ``` /download?dataSource=dataSourceName&schema=schemaName&table=tableName&blobColName=colName&fileName=colName&idName=IdColName&idVal=val``` -
To view the Blob Content that presents in the table
This API accepts 7 query parameters:
  1. dataSource = Data source name.
  2. schema = Schema/Database name.
  3. table = table name.
  4. blobColName = Column name of where the Blob data is present.
  5. fileName = Column name of the File name that is present in the table.
  6. idName = Column name of the ID or Unique & Not Null column name.
  7. idVal = Value of idName Column name.   

7.  ``` /query?dataSource=dataSourceName&schema=schemaName&table=tableName&blobColName=colName&fileName=colName&idName=IdColName&query=select * from table``` -
To view the table details or to run a Custom query:
This API will write all the fetched data in the browser
dataSource, schema, and query are mandatory fields.
If we want to view or download the blob content that is present in the columns we need to specify below query parameters
table, blobColName, fileName, idName

This API accepts 8 query parameters:
  1. dataSource = Data source name.
  2. schema = Schema/Database name.
  3. table = table name.
  4. blobColName = Column name of where the Blob data is present.
  5. fileName = Column name of the File name that is present in the table.
  6. idName = Column name of the ID or Unique & Not Null column name.
  7. query = Custom Query
---

## Server

By default, the server runs on port - ```9090```. 
This can be changed in the ```application.properties``` file.

---

## Connection to External Table

The server's default connection parameters are set as follows: it will attempt to connect to the host ```localhost``` on port ```10000```, with the ```username and password``` both set to ```null```.
However, these settings can be modified in the ```property.json``` file. 
Alternatively, the ```/data-source ``` API can add new server details.
{
    "dataSource": "dataSourceName",
    "host": "hostname",
    "port": 10000,
    "userName": "",
    "password": ""
}


---
# ArchLenz

# ArchLenz
# ArchLenz

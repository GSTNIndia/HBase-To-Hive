### **Introduction:**

This project is for Transferring data from Hbase table to different targets like HDFS file/ Hive table / another Hbase Table. 

### **Background:**

HBase is the Hadoop database, a distributed, scalable, big data store. It is a NoSQL database and is useful for storing key value pairs and accessed by row keys. 

Though below approaches can be followed to migrate data out of HBase, they come with some limitations as below:

1.	HBase provides utilities like copyTable and ‘Export snapshot’ for exporting data from HBase tables. However, the primary purpose of the utilities is to create backups or to duplicate a table data. For example, the utilities cannot be used to copy a HBase table into a Hive table. 
2.	HBase table can be accessed from Hive. However, this requires HBase table to use only predefined column names and no dynamic column names

With this solution it is possible to migrate data including dynamic columns and JSON columns present in the HBase table.


### **Applicability of the solution:**

The solution is useful to migrate some or all of a source HBase table data to a target. All columns or some columns (identified by pattern) in source schema can be considered for selection. The target can be a HDFS File or a Hive table or a HBase table.

### **Key Benefits:**

Framework overcomes following limitations of the existing systems:

1.  Migrating HBase data into targets like Hive, HDFS:
The framework has target adapters defined for Hive, HDFS, HBase. These target adapters transform the data as per the target format and then write it into target. Framework provides Target Adapter as an interface. So it is possible to add your own target adapter for targets apart from the supported ones, by implementing this interface.

2.	Support for tables having Dynamic Schema: 
The framework supports defining dynamic columns in source table schema and source table adapter interprets these dynamic columns and fetches them accordingly from the source HBase table.

3.	Support for JSON columns: 
The Framework allows fetching Json fields as target columns. 
The framework has JSON adapter component. It parses the JSON read from source table and fetches the desired columns defined in the target schema. It’s a generic Json parser that supports all types of Json like json’s having list, nested Json, Json array etc.

4.	Specifying filter conditions and select columns:
The framework allows specifying filter conditions and select columns in JobConfig.xml file. Using this information framework fetches only the desired data from the source table. Filter conditions can be defined in same way as we define conditions in the where clause of SQL query using conditional operators (and/or), relational operators (<,>, = etc.). Filter conditions can be based on static columns, dynamic columns or even on row key components.

### **License:**
This project is licensed under Apache License version 2.0. This project depends upon other third party open source components which are not bundled with this project and they have their own license terms. Please refer LICENSE-THIRD-PARTY file for more information. 
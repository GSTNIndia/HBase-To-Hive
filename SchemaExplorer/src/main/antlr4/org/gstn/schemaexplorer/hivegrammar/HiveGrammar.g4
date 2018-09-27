/*******************************************************************************
 * Copyright 2018 Goods And Services Tax Network
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 *  
 * grammar to parse hive schema
   partition columns are optional
   e.g. 
   schema_name=database=>fp:string:"default_val",stin:string:,rtin:string:=>fp:string:"default_val";
 * 
 * Schema name will be used as table name for hive
 *
***********************************************************************************************************/


grammar HiveGrammar;

input:
hiveTable+
;

hiveTable:
schemaName '=' databaseName IDENTIFIER multipleColumnList optionalPartition optionalStorage';'
;

schemaName:
STRICTSTRING
;

databaseName:
STRICTSTRING
;

multipleColumnList:
columnList (',' columnList)*
;

columnList:
('PARENT' '=' parentPath '>' 'COLUMNS' '=')? columnsCSV
;

parentPath:
parent ('#' parent)*
;

parent:
STRICTSTRING
| NUMBER
;

columnsCSV:
column (',' column)*
;

column:
columnName ':' columnDataType ':' columnDefaultValue
;

columnName:
STRICTSTRING
| jsonField
;

jsonField:
'JSON.' STRICTSTRING
| 'JSON.' NUMBER
;

columnDataType:
STRICTSTRING
| /* default data type is STRING */
;

columnDefaultValue:
'"' DATE '"'
| '"' STRICTSTRING '"'
| '"' NUMBER '"'
|
;

optionalPartition:
(',' 'PARTITION' '=' multiplePartitionList)?
;

multiplePartitionList:
partitionList (',' partitionList)*
;

partitionList:
('PARENT' '=' parentPath '>' 'COLUMNS' '=')? columnsCSV
;

optionalStorage:
(',' 'STORAGE' '=' STRICTSTRING)?
;

IDENTIFIER:
'->'
;

STRICTSTRING:
[a-zA-Z0-9_]*[a-zA-Z_][a-zA-Z0-9_]*
;

NUMBER:
'-'? INT
| '-'? INT '.' DEC
;

INT:
'0'
| [1-9] [0-9]*
;

DEC:
[0-9]+
;

DATE:
YEAR '-' MONTH '-' DAY
| YEAR '/' MONTH '/' DAY
| YEAR '.' MONTH '.' DAY
| YEAR '-' MONTH
;

YEAR:
[1-9] [0-9] [0-9] [0-9]
;

MONTH:
[0] [1-9]
| [1] [0-2]
;

DAY:
[0] [1-9]
| [1-2] [0-9]
| [3] [0-1]
;

WS: 
[ \t\r\n]+ -> channel(HIDDEN)
;
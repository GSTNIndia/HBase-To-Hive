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
 * grammar to parse custom hbase table schema (accommodating dynamic columns)
 * e.g.
 *
 * schemaName:tableName->RK=|:hash(rtin)[0:7]|fy|period|rtin|rectype|stin -> D.JSN<X>:json::snappy,D.summary:string::->X=|:prov|fy|inum;
 * schemaName:tableName->RK=#:hash(rtin)[0:7]#fy->D.JSN<X>:json::snappy,D.summary:string::->X=#:prov#fy#inum;
 * schemaName:tableName->RK=$:hash(rtin)[0:7]$fy->D.JSN<X>:json::snappy,D.summary:string::->X=$:prov$fy$inum;
 * schemaName:tableName->RK=>:hash(rtin)[0:7]>fy->D.JSN<X>:json::snappy,D.summary:string::->X=>:prov>fy>inum;
 * schemaName:tableName->RK==:hash(rtin)[0:7]=fy->D.JSN<X>:json::snappy,D.summary:string::->X==:prov=fy=inum;
***********************************************************************************************************/


grammar HBaseTableGrammar;

input:
hbaseTable+
;

hbaseTable:
schemaName ':' hbaseTableName IDENTIFIER rowKeyMetaData IDENTIFIER multipleColumnList (dynamicPartMetadata)? ';'
;

schemaName:
STRICTSTRING
;

hbaseTableName:
STRICTSTRING
;

rowKeyMetaData:
ROWKEY rowKeySeparator ':' rowKey
| ROWKEY rowField
;

ROWKEY:
'RK='
;

IDENTIFIER:
'->'
;

rowKey:
rowField (rowKeySeparator rowField)+
;

rowField:
STRICTSTRING
| literal
| hashedField bitPrefix
;

rowKeySeparator:
SEPARATOR
| '>'
| '<'
| '='
| '-'
| ','
| '#'
;

literal:
STRICTSTRING '=' '"' string '"'
;

hashedField:
'hash' '(' STRICTSTRING ')'
;

bitPrefix:
'[' NUMBER ':' NUMBER ']'
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
columnFamily '.' staticColumn ':' columnDataType ':' columnDefaultValue ':' compressionType
| columnFamily '.' dynamicColumn ':' columnDataType ':' columnDefaultValue ':' compressionType
;

columnFamily:
STRICTSTRING
;

staticColumn:
STRICTSTRING
| jsonField
;

jsonField:
'JSON.' STRICTSTRING
| 'JSON.' NUMBER
;

dynamicColumn:
nullableString '<' STRICTSTRING '>' nullableString
;

nullableString:
(string)?
;

string:
STRICTSTRING
| NUMBER
;

dynamicPartMetadata:
IDENTIFIER STRICTSTRING '=' dynamicPartSeparator ':' dynamicPart
| IDENTIFIER STRICTSTRING '=' dynamicPartName
;

dynamicPart:
dynamicPartName (dynamicPartSeparator dynamicPartName)+
;

dynamicPartName:
STRICTSTRING
;

dynamicPartSeparator:
SEPARATOR
| '>'
| '<'
| '='
| '-'
| ','
| '#'
;

SEPARATOR:
~[a-zA-Z0-9 \t\r\n]
;

columnDataType:
dataType
| dataType '(' format ')'
| /* default data type is STRING */
;

columnDefaultValue:
'"' DATE '"'
| '"' STRICTSTRING '"'
| '"' NUMBER '"'
|
;

dataType:
STRICTSTRING
;

format:
DATE_CHAR '-' DATE_CHAR '-' DATE_CHAR
| DATE_CHAR '/' DATE_CHAR '/' DATE_CHAR
| DATE_CHAR '.' DATE_CHAR '.' DATE_CHAR
;

compressionType:
STRICTSTRING
| /* no compression assumed by default */
;

DATE_CHAR:
'dd'
| 'DD'
| 'MM'
| 'yyyy'
;

STRICTSTRING:
[a-zA-Z0-9_]*[_a-zA-Z][a-zA-Z0-9_]*
;

NUMBER:
'0'
| '-'? INT
| '-'? INT '.' DEC
;

INT:
[1-9] [0-9]*
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
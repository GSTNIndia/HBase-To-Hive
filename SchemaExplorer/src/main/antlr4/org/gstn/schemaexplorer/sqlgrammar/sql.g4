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
 * grammar to parse sql.
 * e.g.
 *
 * select a,b,c from abc where a = 1 and b = 1
 *
***********************************************************************************************************/


grammar sql;
selectStmt:
'select ' starOrColumnList ' from ' tableName  (whereClause)?
;

starOrColumnList:
starList 
| columnList 
;

starList:
star (',' star)*
;

star:
'*'
| string '.' '*'
;

columnList:
column (',' column)*
;

column:
string '.' staticColumn 
| string '.' dynamicColumn 
| staticColumn 
| dynamicColumn
;

staticColumn:
STRICTSTRING
;

dynamicColumn:
nullableString '<' STRICTSTRING '>' nullableString
;

tableName:
STRICTSTRING
;

whereClause:
' where ' conditions
;

conditions:
condition
| conditions operator conditionsInBracket
| conditionsInBracket operator conditions
| conditionsInBracket operator conditionsInBracket 
| condition  operator  conditions 
| conditionsInBracket
;

conditionsInBracket:
'(' condition  operator  conditions ')'
| '(' conditions  operator  condition ')'
| '(' conditions  operator  conditions ')'
;

condition:
column  conditionalOperator  STRING_VALUE 
| column conditionalOperator  NUMBER
;

conditionalOperator:
'='|'<='|'>='|'<'|'>'|'<>'|'!='|'REGEXP'|'NOT REGEXP'
;

operator:
'AND'|'OR'|'and'|'or'
;

nullableString:
string
|
;

string:
STRICTSTRING
| NUMBER 
;

number:
NUMBER
;

STRING_VALUE:
'"' (~[\r\n"] | '""')* '"' | '\'' (~[\r\n\'] | '\'\'')* '\''
;

STRICTSTRING:
[a-zA-Z0-9_-]*[_a-zA-Z-][a-zA-Z0-9_-]*
;

NUMBER:
[0-9]+
;

WS: 
[ \t\r\n]+ -> channel(HIDDEN)
;

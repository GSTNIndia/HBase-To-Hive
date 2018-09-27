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
 ******************************************************************************/
package org.gstn.schemaexplorer.hbase;

import static org.junit.Assert.assertEquals;

import org.gstn.schemaexplorer.exception.InvalidSchemaException;
import org.gstn.schemaexplorer.hbase.HBaseColumn;
import org.gstn.schemaexplorer.hbase.HBaseColumnList;
import org.junit.Test;

public class HBaseColumnListTest {
	
	@Test
	public void test() throws InvalidSchemaException {
		HBaseColumn hc1 = new HBaseColumn("D", "JSN", "json", "", "", "snappy", true);
		HBaseColumn hc2 = new HBaseColumn("D", "RC", "", "X", "string", "", "Reverse Charge", "", false);
		HBaseColumn hc3 = new HBaseColumn("S", "", "DTL", "X", "string", "", "Details", "", false);
		HBaseColumn hc4 = new HBaseColumn("S", "MD", "", "", "", "snappy", true);
		
		HBaseColumnList hcl = new HBaseColumnList();
		hcl.addColumn(null, hc1);
		hcl.addColumn(null, hc2);
		hcl.addColumn(null, hc3);
		hcl.addColumn(null, hc4);
		
		assertEquals(true, hcl.isColumnPresent("D", "RCX"));
		assertEquals(false, hcl.isColumnPresent("D", "RC"));
		assertEquals(false, hcl.isColumnPresent("D", "MD"));
		assertEquals(true, hcl.isColumnPresent("S", "XDTL"));
		
		assertEquals("string", hcl.getColumnDataType("D", "RCX"));				
		assertEquals(String.class, hcl.getColumnDataTypeClass("D", "RCX"));
		assertEquals(true, hcl.isDynamicColumn("S", "XDTL"));
		assertEquals(false, hcl.isDynamicColumn("D", "JSN"));
		
	}
	
}

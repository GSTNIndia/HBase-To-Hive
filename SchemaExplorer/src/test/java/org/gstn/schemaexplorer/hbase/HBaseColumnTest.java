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

import org.gstn.schemaexplorer.hbase.HBaseColumn;
import org.junit.Test;


public class HBaseColumnTest {

	@Test
	public void test() {
		HBaseColumn hc1 = new HBaseColumn("D", "JSN", "json", "",  "", "snappy", true);
		
		assertEquals("", hc1.getColumnDefaultValue());
		assertEquals(true, hc1.isJsonField());
		assertEquals("JSN", hc1.getColumnName());
		assertEquals(null, hc1.getDynamicPrefix());
		assertEquals(null, hc1.getDynamicSuffix());
		assertEquals("D", hc1.getColumnFamily());
		assertEquals(false, hc1.isDynamicColumn());
//		This is supported in junit 5
//		assertThrows(HQLException.class, () -> {
//			hc1.getDynamicComponent();
//		});

		HBaseColumn hc2 = new HBaseColumn("D", "Abc", "deF", "X", "string", "",  "abcdef", "", false);
		
		assertEquals("abcdef", hc2.getColumnDefaultValue());
		assertEquals(false, hc2.isJsonField());
		assertEquals("AbcXdeF", hc2.getColumnName());
		assertEquals("Abc", hc2.getDynamicPrefix());
		assertEquals("deF", hc2.getDynamicSuffix());
		assertEquals("D", hc2.getColumnFamily());
		assertEquals(true, hc2.isDynamicColumn());
		assertEquals("X", hc2.getDynamicComponent());
	}
	
}

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
package org.gstn.schemaexplorer.hive;

import static org.junit.Assert.assertEquals;

import org.gstn.schemaexplorer.hive.HiveColumn;
import org.gstn.schemaexplorer.hive.HiveColumnList;
import org.junit.Test;

public class HiveColumnListTest {
	
	@Test
	public void test() {
		HiveColumn hc1 = new HiveColumn("RC", "string", "N", false, false);
		HiveColumn hc2 = new HiveColumn("txval", "decimal", "", true, false);
		HiveColumn hc3 = new HiveColumn("iamt", "decimal", "", true, false);
		HiveColumn hc4 = new HiveColumn("pos", "string", "", true, false);
		
		HiveColumnList hcl = new HiveColumnList();
		hcl.addColumn(null, hc1);
		hcl.addColumn(null, hc2);
		hcl.addColumn(null, hc3);
		hcl.addColumn(null, hc4);
		
		assertEquals("N", hcl.getColumnDefaultValue("RC"));
	}

}

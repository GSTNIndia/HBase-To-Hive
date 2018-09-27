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
package org.gstn.schemaexplorer.hdfs;

import static org.junit.Assert.assertEquals;

import org.gstn.schemaexplorer.hdfs.HdfsColumn;
import org.gstn.schemaexplorer.hdfs.HdfsColumnList;
import org.junit.Test;

public class HdfsColumnListTest {
	
	@Test
	public void test() {
		HdfsColumn hc1 = new HdfsColumn("RTYPE", "string", "R1", true);
		HdfsColumn hc2 = new HdfsColumn("txval", "decimal", "", true);
		HdfsColumn hc3 = new HdfsColumn("iamt", "decimal", "", true);
		HdfsColumn hc4 = new HdfsColumn("pos", "string", "", true);
		
		HdfsColumnList hcl = new HdfsColumnList();
		hcl.addColumn(null, hc1);
		hcl.addColumn(null, hc2);
		hcl.addColumn(null, hc3);
		hcl.addColumn(null, hc4);
		
		assertEquals("R1", hcl.getColumnDefaultValue("RTYPE"));
	}

}

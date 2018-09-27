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

import org.gstn.schemaexplorer.hbase.Rowkey;
import org.gstn.schemaexplorer.hbase.RowkeyField;
import org.junit.Test;

public class RowkeyTest {
	
	@Test
	public void test() {
		RowkeyField rkf1 = new RowkeyField("rtin", (short)0, (short)7);
		RowkeyField rkf2 = new RowkeyField("fp", "", false);
		RowkeyField rkf3 = new RowkeyField("stin", "", false);
		RowkeyField rkf4 = new RowkeyField("rtin", "", false);
		RowkeyField rkf5 = new RowkeyField("statecode", "07", true);
		
		Rowkey rk1 = new Rowkey();
		rk1.addRowkeyField(rkf1);
		rk1.addRowkeyField(rkf2);
		rk1.addRowkeyField(rkf3);
		rk1.addRowkeyField(rkf4);
		rk1.addRowkeyField(rkf5);
		
		Rowkey rk2 = new Rowkey();
		rk2.addRowkeyField(rkf2);
		rk2.addRowkeyField(rkf3);
		rk2.addRowkeyField(rkf4);
		rk2.addRowkeyField(rkf5);
		
		assertEquals(true, rk1.isHashed());
		assertEquals(false, rk2.isHashed());
		
		assertEquals(1, rk1.getHashSizeBytes());
		
		assertEquals(true, rk1.isColumnPresent("rtin"));
		assertEquals(false, rk2.isColumnPresent("bucketID"));
		
		assertEquals(false, rk2.isAValidRowKey(new String[]{"fp", "stin", "rtin"}));
	}

}

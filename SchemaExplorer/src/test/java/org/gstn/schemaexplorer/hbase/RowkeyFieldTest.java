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

import org.gstn.schemaexplorer.hbase.RowkeyField;
import org.junit.Test;

public class RowkeyFieldTest {
	
	@Test
	public void test() {	
		RowkeyField rkf1 = new RowkeyField("id", "", false);
		
		assertEquals(false, rkf1.isLiteral());
		assertEquals(false, rkf1.isHashed());
		assertEquals(null, rkf1.getLiteralValue());
		assertEquals(0, rkf1.getBitStart());
		assertEquals(-1, rkf1.getBitEnd());
		
		RowkeyField rkf2 = new RowkeyField("id", (short)0, (short)7);
		
		assertEquals(false, rkf2.isLiteral());
		assertEquals(true, rkf2.isHashed());
		assertEquals(null, rkf2.getLiteralValue());
		assertEquals(0, rkf2.getBitStart());
		assertEquals(7, rkf2.getBitEnd());		
	}
	
}

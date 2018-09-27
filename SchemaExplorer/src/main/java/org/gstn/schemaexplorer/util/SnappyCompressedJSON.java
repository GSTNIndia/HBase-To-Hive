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
package org.gstn.schemaexplorer.util;

import java.io.IOException;

/**
 * Class to identify JSON columns in the HBase table, which are also snappy compressed
 *
 */
public class SnappyCompressedJSON extends JSON {
	public static String toString(byte[] value) throws IOException{
		return SnappyCompressedType.toString(value);
	}
	
	public static byte[] toBytes(String value) throws IOException{
		return SnappyCompressedType.toBytes(value);
	}
}

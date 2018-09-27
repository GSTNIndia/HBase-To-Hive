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
package org.gstn.hbasetohive.adapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.gstn.hbasetohive.entity.TargetAdapterWrapper;
import org.gstn.hbasetohive.pojo.DeletionMetadata;
import org.gstn.schemaexplorer.entity.DataRecord;

import scala.Tuple2;

/**
 * This is an abstract class which declares methods for writing data into the 
 * target.
 */
public abstract class TargetAdapter {
	
	TargetAdapter() {
	}

	/** 
     * This abstract method is for writing input datarecord into target.
     * @param targetModel
     * 			TargetModel instance for the desired target
     * @param record
     * 			DataRecord instance which has data from source hbase row to be 
     * 			written into target
     * @return	
     * 			The restructured data record as per the TargetModel.
	 * @throws Exception
	 */
	abstract DataRecord writeRow(TargetModel targetModel, DataRecord record) throws Exception;

	/** 
     * This abstract method is for performing flush related activities on the 
     * target.
     */ 
	public abstract void flush() throws IOException;

	/** 
     * This abstract method is for performing close related operation on the 
     * target.
     */ 
	public abstract void close() throws IOException;

	
	/**
	 * This method creates TargetAdapterWrapper for all the input hbase 
	 * result objects that belong to a spark partition.
	 * @param partition
	 * 			Iterator over Tuple objects of all the hbase result objects 
	 * 			that belong to a spark partition.
	 * @return
	 * 			List of TargetAdapterWrapper, which has only one 
	 * 			targetAdapterWrapper instance containing all the input result 
	 * 			objects. 
	 * @throws IOException
	 */
	public List<TargetAdapterWrapper> createTargetAdapterWrapper(
			Iterator<Tuple2<ImmutableBytesWritable, Result>> partition) throws IOException {

		List<TargetAdapterWrapper> list = new ArrayList<>();
		list.add(new TargetAdapterWrapper(this, partition));
		return list;
	}

	
	/**
	 * This method is for handling deletions as per the target 
	 * @param deleteRowList
	 * @throws Exception 
	 */
	public abstract DeletionMetadata processDeleteRowList(TargetModel targetModel, List<DataRecord> deleteRowList) throws Exception;
}

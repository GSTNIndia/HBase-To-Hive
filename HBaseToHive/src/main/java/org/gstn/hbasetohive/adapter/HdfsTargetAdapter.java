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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.TaskContext;
import org.gstn.hbasetohive.pojo.DeletionMetadata;
import org.gstn.schemaexplorer.entity.DataRecord;

/**
 * This class implements TargetAdapter for HDFS. It creates target row in 
 * string format from source DataRecord and writes it into HDFS file.
 *
 */
public class HdfsTargetAdapter extends TargetAdapter {

	private BufferedWriter insertBufferedWriter, deleteAllBufferedWriter, 
						deleteSingleBufferedWriter;
	
	private final String insertFilePath, deleteAllFilePath, deleteSingleFilePath, hdfsURL;
	
	private final boolean overwriteHDFSFile = true;
	
	private static final String separator = "\u0001";
	private static final String quoteChar = "";

	/**
	 * @param hdfsURL
	 * 			Namenode URL for the hdfs cluster where we want to write 
	 * 			hdfs file
	 * @param basePath
	 * 			HDFS directory path under which we want the HDFS data to be written.  
	 * @throws IOException
	 */
	public HdfsTargetAdapter(String hdfsURL, String basePath) {

		if (!basePath.endsWith(File.separator)) {
			basePath = basePath.concat(File.separator);
		}
		int partitionId = TaskContext.getPartitionId();
		insertFilePath = basePath + "insert_" +partitionId;
		deleteAllFilePath = basePath + "delete_all_" +partitionId;
		deleteSingleFilePath = basePath + "delete_single_" +partitionId;

		this.hdfsURL = hdfsURL;
		
	}

	@Override
	public DataRecord writeRow(TargetModel targetModel, DataRecord record) throws Exception {
		DataRecord reorderedRecord = targetModel.structureDataRecord(record);

		String row = reorderedRecord.getRecord(separator, quoteChar);
		
		if(insertBufferedWriter==null){
			insertBufferedWriter = getBufferedWriter(insertFilePath);
		}
		
		insertBufferedWriter.write(row);
		insertBufferedWriter.newLine();

		return reorderedRecord;
	}

	@Override
	public void flush() throws IOException {
		if(insertBufferedWriter!=null)
			insertBufferedWriter.flush();
		
		if(deleteAllBufferedWriter!=null)
			deleteAllBufferedWriter.flush();
		
		if(deleteSingleBufferedWriter!=null)
			deleteSingleBufferedWriter.flush();
	}

	@Override
	public void close() throws IOException {
		if(insertBufferedWriter!=null)
			insertBufferedWriter.close();
		
		if(deleteAllBufferedWriter!=null)
			deleteAllBufferedWriter.close();
		
		if(deleteSingleBufferedWriter!=null)
			deleteSingleBufferedWriter.close();
	}

	@Override
	public DeletionMetadata processDeleteRowList(TargetModel targetModel, List<DataRecord> deleteRowList) throws Exception {
		boolean deleteAllAdded=false;
		boolean deleteSingleAdded=false;
		
		for (DataRecord deleteRecord : deleteRowList) {
			DataRecord structuredRecord = targetModel.structureDeleteRecord(deleteRecord);
			String row = structuredRecord.getKeyAsString(separator, quoteChar);
			if(!structuredRecord.isDynamicPartsInKey()){
				
				//add to delete all
				if(deleteAllBufferedWriter==null){
					deleteAllBufferedWriter = getBufferedWriter(deleteAllFilePath);
				}
				deleteAllBufferedWriter.write(row);
				deleteAllBufferedWriter.newLine();
				
				deleteAllAdded=true;
			}else{
				//add to delete single
				if(deleteSingleBufferedWriter==null){
					deleteSingleBufferedWriter = getBufferedWriter(deleteSingleFilePath);
				}
				deleteSingleBufferedWriter.write(row);
				deleteSingleBufferedWriter.newLine();
				
				deleteSingleAdded=true;
			}
		}
		
		return new DeletionMetadata(deleteAllAdded, deleteSingleAdded);
	}
	
	private BufferedWriter getBufferedWriter(String hdfsFilePath) throws IOException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsURL);
		
		Path path = new Path(hdfsFilePath);
		FSDataOutputStream os = FileSystem.get(conf).create(path, overwriteHDFSFile);
		return new BufferedWriter(new OutputStreamWriter(os));
	}
	
}


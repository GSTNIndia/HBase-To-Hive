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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gstn.hbasetohive.pojo.DeletionMetadata;
import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.hbase.HBaseTableExplorer;

/**
 * This class implements TargetAdapter for Hbase. It transforms DataRecord 
 * into Hbase put and writes it into target Hbase table.
 *
 */
public class HBaseTargetAdapter extends TargetAdapter {
	
	private final String targetSchema;
    private final String targetTable;
    private final HBaseTableExplorer hBaseExplorer;
    
    private Connection hBaseConnection;
    private BufferedMutator bufferedMutator;
    private final int DELETE_BATCH_SIZE = 1000;

    /**
     * @param ZKQuorum 
     * 			HBase zookeeper quorum for target HBase table
     * @param targetSchema
     * 			Target HBase schema name
     * @param hBaseExplorer
     * 			HBaseTableIR object having information about user defined schemas
     * @throws IOException
     */
    public HBaseTargetAdapter(String ZKQuorum, String targetSchema, HBaseTableExplorer hBaseExplorer) throws IOException {
    	
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZKQuorum);

        this.hBaseConnection = ConnectionFactory.createConnection(conf);
        this.targetSchema = targetSchema;
        this.targetTable = hBaseExplorer.getTableName(targetSchema);
        this.bufferedMutator = hBaseConnection.getBufferedMutator(TableName.valueOf(targetTable));
        this.hBaseExplorer = hBaseExplorer;
    }

    /** 
     * This method creates HBase put from input datarecord and writes it into target HBase table.
     * @param targetModel
     * 			HbaseTargetModel instance
     * @param ipRecord
     * 			DataRecord instance which has data from source HBase row to be 
     * 			written into target HBase row
     * @return	
     * 			The restructured data record as per the HbaseTargetModel.
     * @see org.gstn.hbasetohive.adapter.TargetAdapter#writeRow(org.gstn.hbasetohive.adapter.TargetModel, org.gstn.schemaexplorer.entity.DataRecord)
     */
    @Override
    public DataRecord writeRow(TargetModel targetModel, DataRecord ipRecord) throws Exception {
        String rowKeySeparator = hBaseExplorer.getRowKeySeparator(targetSchema);
        DataRecord reorderedRecord = targetModel.structureDataRecord(ipRecord);

        Put put = reorderedRecord.getHBasePut(hBaseExplorer, targetSchema, rowKeySeparator);

        bufferedMutator.mutate(put);
        return reorderedRecord;
    }

    /**
     * This method performs flush on bufferedMutator object used for writing 
     * rows into target HBase table.
     * @see org.gstn.hbasetohive.adapter.TargetAdapter#flush()
     */
    @Override
    public void flush() throws IOException {
        bufferedMutator.flush();
    }

    /**
     * This method closes the bufferedMutator and connection objects used for 
     * writing into target HBase table.
     * @see org.gstn.hbasetohive.adapter.TargetAdapter#close()
     */
    @Override
    public void close() throws IOException {
        bufferedMutator.close();
        hBaseConnection.close();
    }

	@Override
	public DeletionMetadata processDeleteRowList(TargetModel targetModel, List<DataRecord> deleteRowList) throws Exception {
		String rowKeySeparator = hBaseExplorer.getRowKeySeparator(targetSchema);
		
		boolean deleteAllAdded=false;
		boolean deleteSingleAdded=false;
		
		List<Delete> deleteList = new ArrayList<>();
		for (DataRecord deleteRecord : deleteRowList) {
			
			DataRecord reorderedRecord = targetModel.structureDataRecord(deleteRecord);
			
			if(reorderedRecord.isRegexInKey()){
				//create scan using regex key for identifying rows to be deleted
				
				//sending escaped rowKeySeparator as we are generating regex
				byte[] rowKeyRegex = reorderedRecord.getSaltedKey("\\"+rowKeySeparator, "");
				ResultScanner scanner = getRowKeyRegexScanner(Bytes.toString(rowKeyRegex), targetTable);
				
				org.apache.hadoop.hbase.client.Result result = scanner.next();
				
				while(result!=null && result.isEmpty()){
					deleteList.add(new Delete(result.getRow()));
					checkBatchSizeAndClear(deleteList);
					result = scanner.next();
				}
			}else{
				byte[] rowKey = reorderedRecord.getSaltedKey(rowKeySeparator, "");
				deleteList.add(new Delete(rowKey));
				checkBatchSizeAndClear(deleteList);
			}
			
			if(!deleteRecord.isDynamicPartsInKey()){
				deleteAllAdded=true;
			}else{
				deleteSingleAdded=true;
			}
		}
        
		if(!deleteList.isEmpty()){
			bufferedMutator.mutate(deleteList);
		}
		
		return new DeletionMetadata(deleteAllAdded, deleteSingleAdded);
	}
	
	private void checkBatchSizeAndClear(List<Delete> deleteList) throws IOException {
		if(deleteList.size()>=DELETE_BATCH_SIZE){
			bufferedMutator.mutate(deleteList);
			deleteList.clear();
		}
		
	}

	private ResultScanner getRowKeyRegexScanner(String rowKeyRegex, String tableName) throws IOException {
		Scan scan = new Scan();
		
		FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		filters.addFilter(new KeyOnlyFilter());
		filters.addFilter(new FirstKeyOnlyFilter());
		
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex));
		filters.addFilter(rowFilter);

		scan.setFilter(filters);
		
		//disabling cache blocks as we are doing full table scan
		scan.setCacheBlocks(false);

		Table table = hBaseConnection.getTable(TableName.valueOf(tableName));

		return table.getScanner(scan);
	}
    
}

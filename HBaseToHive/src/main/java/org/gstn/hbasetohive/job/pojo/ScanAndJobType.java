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
package org.gstn.hbasetohive.job.pojo;

import org.apache.hadoop.hbase.client.Scan;

public class ScanAndJobType {
	
	private Scan scan;
	private long minTimeStamp;
	private boolean incremental;
	
	public ScanAndJobType(Scan scan, long minTimeStamp, boolean incremental) {
		super();
		this.scan = scan;
		this.minTimeStamp = minTimeStamp;
		this.incremental = incremental;
	}
	
	public Scan getScan() {
		return scan;
	}
	
	public boolean isIncremental() {
		return incremental;
	}
	
	public long getMinTimeStamp() {
		return minTimeStamp;
	}
	
}

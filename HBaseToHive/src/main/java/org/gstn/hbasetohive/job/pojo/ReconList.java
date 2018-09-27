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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gstn.hbasetohive.exception.ValidationException;

@SuppressWarnings("serial")
public class ReconList implements Serializable {

	private List<Recon> recon;

	public List<Recon> getRecon() {
		return recon;
	}

	public void setRecon(List<Recon> recon) {
		this.recon = recon;
	}

	public ReconList() {
		this.recon = new ArrayList<>();
	}

	public Map<String, List<String>> getReconColumnOpMap() {

		Map<String, List<String>> reconColumnOpMap = new HashMap<>();

		for (Recon recon : recon) {

			String column = recon.getReconColumn();
			String operation = recon.getReconOperation();

			if (reconColumnOpMap.get(column) == null) {
				reconColumnOpMap.put(column, new ArrayList<>());
			}
			reconColumnOpMap.get(column).add(operation);
		}
		return reconColumnOpMap;
	}

	public void validate(int counter, String jcFilePath) throws ValidationException {

		if (recon == null || recon.isEmpty()) {
			throw new ValidationException(
					"No <recon> specified inside <reconlist> of job(" + counter + ") in config file: " + jcFilePath);
		}
		int rcount = 1;
		for (Recon recon : recon) {
			if (recon.getReconColumn() == null) {
				throw new ValidationException("<reconColumn> not specified inside recon(" + rcount + ")of job("
						+ counter + ") in config file: " + jcFilePath);
			}
			if (recon.getReconColumn().isEmpty()) {
				throw new ValidationException("Please specify a valid string in <reconColumn> inside recon(" + rcount
						+ ") of job(" + counter + ") in config file: " + jcFilePath);
			}
			if (recon.getReconOperation() == null) {
				throw new ValidationException("<reconOperation> not specified inside recon(" + rcount + ")of job("
						+ counter + ") in config file: " + jcFilePath);
			}
			if (recon.getReconOperation().isEmpty()) {
				throw new ValidationException("Please specify a valid string in <reconOperation> inside recon(" + rcount
						+ ") of job(" + counter + ") in config file: " + jcFilePath);
			}
			if (!(recon.getReconOperation().equalsIgnoreCase("sum") || recon.getReconOperation().equals("count"))) {
				throw new ValidationException("Please specify one of count or sum in <reconOperation> inside recon("
						+ rcount + ") of job(" + counter + ") in config file: " + jcFilePath);
			}
			rcount++;
		}
	}

}

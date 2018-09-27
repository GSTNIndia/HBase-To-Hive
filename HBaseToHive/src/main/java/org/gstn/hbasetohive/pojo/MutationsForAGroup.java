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
package org.gstn.hbasetohive.pojo;

import java.io.Serializable;

import org.gstn.schemaexplorer.entity.DataRecord;
import org.gstn.schemaexplorer.entity.Tuple;
import org.gstn.schemaexplorer.exception.InvalidColumnException;

public class MutationsForAGroup implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6110508501951664328L;

	private DataRecord putMutations;
	private DataRecord deleteColumnMutations;

	public MutationsForAGroup() {
		this.putMutations = new DataRecord();
		this.deleteColumnMutations = new DataRecord();
	}

	public DataRecord getPutMutations() {
		return putMutations;
	}

	public DataRecord getDeleteColumnMutations() {
		return deleteColumnMutations;
	}

	public boolean isMutationPresentForColumn(String cf, String cn) {
		if (putMutations.isColumnNamePresent(cf, cn) || deleteColumnMutations.isColumnNamePresent(cf, cn)) {
			return true;
		} else {
			return false;
		}
	}

	public void addTupleToDeleteMutation(Tuple t) throws InvalidColumnException {
		deleteColumnMutations.addTupleToColumn(t.getColumnFamily(), t.getColumnName(), t);
	}

	public void addTupleToPutMutation(Tuple t) throws InvalidColumnException {
		putMutations.addTupleToColumn(t.getColumnFamily(), t.getColumnName(), t);
	}
}

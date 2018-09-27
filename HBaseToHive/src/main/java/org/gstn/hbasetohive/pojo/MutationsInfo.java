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
import java.util.Map;

public class MutationsInfo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8136879341688772117L;

	private Map<String, MutationsForAGroup> mutations;
	private boolean mutationForStaticPresent;
	private boolean mutationForDyanmicPresent;

	public MutationsInfo(Map<String, MutationsForAGroup> mutations, boolean mutationForStaticPresent,
			boolean mutationForDyanmicPresent) {
		this.mutations = mutations;
		this.mutationForStaticPresent = mutationForStaticPresent;
		this.mutationForDyanmicPresent = mutationForDyanmicPresent;
	}

	public Map<String, MutationsForAGroup> getMutations() {
		return mutations;
	}

	public void setMutations(Map<String, MutationsForAGroup> mutations) {
		this.mutations = mutations;
	}

	public boolean isMutationForStaticPresent() {
		return mutationForStaticPresent;
	}

	public void setMutationForStaticPresent(boolean mutationForStaticPresent) {
		this.mutationForStaticPresent = mutationForStaticPresent;
	}

	public boolean isMutationForDyanmicPresent() {
		return mutationForDyanmicPresent;
	}

	public void setMutationForDyanmicPresent(boolean mutationForDyanmicPresent) {
		this.mutationForDyanmicPresent = mutationForDyanmicPresent;
	}

}

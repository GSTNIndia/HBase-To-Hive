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
package org.gstn.schemaexplorer.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class stores all the conditions in a structured way
 */
@SuppressWarnings("serial")
public class ConditionTree extends Condition implements Serializable {
	// can contain condition as well as ConditionList
	private List<Condition> conditions;

	private Operator operator;

	public static enum Operator {
		AND, OR
	}

	public ConditionTree() {
		this.conditions = new ArrayList<>();
	}

	private ConditionTree(List<Condition> conditionsList, Operator operator) {
		super();
		this.conditions = conditionsList;
		this.operator = operator;
	}

	public Operator getOperator() {
		return operator;
	}

	public List<Condition> getConditions() {
		List<Condition> conditionList = new ArrayList<>();
		conditionList.addAll(conditions);
		return conditionList;
	}

	/**
	 * This method creates an instance of condition tree with OR as default
	 * operator and returns it
	 * 
	 * @param conditions
	 *            - operator to be used between conditions
	 * @return instance of condition tree
	 */
	public static ConditionTree getInstance(Condition... conditions) {
		return getInstance(Operator.OR.toString(), conditions);
	}

	/**
	 * This method creates an instance of condition tree and returns it
	 * 
	 * @param operator
	 *            - operator to be used between conditions
	 * @param conditions
	 *            - list of conditions
	 * @return instance of condition tree
	 */
	public static ConditionTree getInstance(String operator, Condition... conditions) {
		ConditionTree conditionTree = new ConditionTree(Arrays.asList(conditions),
				Operator.valueOf(operator.toUpperCase()));
		return conditionTree;
	}

	/**
	 * This method return a copy of the condition tree
	 */
	public ConditionTree getDeepCopy() {
		List<Condition> conditionListCopy = new ArrayList<>();

		for (Condition condition : conditions) {
			conditionListCopy.add(condition.getDeepCopy());
		}

		ConditionTree conditionTreeCopy = new ConditionTree(conditionListCopy, operator);

		// create copy of members coming from superclass that we set for this
		// object
		conditionTreeCopy.setEvaluated(this.isEvaluated());
		conditionTreeCopy.setResult(this.getResult());

		return conditionTreeCopy;
	}

	/**
	 * This method returns list of conditions by traversing the whole condition
	 * tree
	 * 
	 * @return list of conditions
	 */
	public List<Condition> getOnlyConditions() {
		List<Condition> conditionList = new ArrayList<>();
		for (Condition condition : conditions) {
			if (condition instanceof ConditionTree) {
				conditionList.addAll(((ConditionTree) condition).getOnlyConditions());
			} else {
				conditionList.add(condition);
			}
		}
		return conditionList;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append("ConditionTree [");
		if (conditions != null && !conditions.isEmpty()) {
			sb.append(conditions.get(0));
			for (int i = 1; i < conditions.size(); i++) {
				sb.append(" " + operator + " " + conditions.get(i) + "\n");
			}
		}
		sb.append(", evaluated: " + isEvaluated() + ", result: " + getResult() + " ]");
		return sb.toString();
	}

}

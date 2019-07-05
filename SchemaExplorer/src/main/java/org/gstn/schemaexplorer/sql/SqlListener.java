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

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.gstn.schemaexplorer.sqlgrammar.sqlBaseListener;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.ColumnListContext;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.ConditionContext;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.ConditionsContext;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.ConditionsInBracketContext;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.OperatorContext;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.StarListContext;
import org.gstn.schemaexplorer.sqlgrammar.sqlParser.WhereClauseContext;

public class SqlListener extends sqlBaseListener implements ParseTreeListener {

	private StringBuilder sb = new StringBuilder();
	private SqlBean sqlBean;

	public SqlListener(SqlBean sqlBean) {
		this.sqlBean = sqlBean;
	}

	@Override
	public void enterTableName(sqlParser.TableNameContext ctx) {
		sb.setLength(0);
		sb.append(ctx.getText());

	}

	@Override
	public void exitTableName(sqlParser.TableNameContext ctx) {
		super.exitTableName(ctx);
		sqlBean.setSchemaName(sb.toString());
		sb.setLength(0);
	}

	@Override
	public void exitColumnList(ColumnListContext ctx) {
		super.exitColumnList(ctx);
		sb.setLength(0);
		sb.append(ctx.getText());

		for (String col : sb.toString().split("\\,")) {
			List<String> column = new ArrayList<>();
			if (col.indexOf(".") != -1) {
				String[] cfC = col.split("\\.");
				column.add(cfC[0]);
				column.add(cfC[1]);

			} else {
				column.add("");
				column.add(col);
			}
			sqlBean.getColumnList().add(column);
		}
	}

	@Override
	public void exitStarList(StarListContext ctx) {
		super.exitStarList(ctx);
		sb.setLength(0);
		sb.append(ctx.getText());

		for (String col : sb.toString().split("\\,")) {
			List<String> column = new ArrayList<>();
			if (col.indexOf(".") != -1) {
				String[] cfC = col.split("\\.");
				column.add(cfC[0]);
				column.add(cfC[1]);

			} else {
				column.add("");
				column.add(col);
			}
			sqlBean.getColumnList().add(column);
		}
	}

	@Override
	public void enterWhereClause(WhereClauseContext ctx) {
		super.enterWhereClause(ctx);
		// fetching conditions
		ConditionsContext conditionsCtx = ctx.getChild(ConditionsContext.class, 0);

		if (conditionsCtx != null) {
			ConditionTree conditionTree = parseConditions(conditionsCtx);

			sqlBean.setConditionTree(conditionTree);
		}
	}

	private ConditionTree parseConditions(ConditionsContext context) {
		ConditionTree conditionTree;

		ConditionsContext conditionsCtx_0 = context.getChild(ConditionsContext.class, 0);

		ConditionsInBracketContext conditionsInBracketCtx_0 = context.getChild(ConditionsInBracketContext.class, 0);

		if (conditionsCtx_0 != null && conditionsInBracketCtx_0 != null) {
			// rule: conditions operator conditions_in_bracket |
			// conditions_in_bracket operator conditions
			ConditionTree conditionTree1 = parseConditions(conditionsCtx_0);
			ConditionTree conditionTree2 = parseConditionsInBracket(conditionsInBracketCtx_0);

			String operator = context.getChild(OperatorContext.class, 0).getText();

			conditionTree = ConditionTree.getInstance(operator, conditionTree1, conditionTree2);

		} else if (conditionsCtx_0 == null && conditionsInBracketCtx_0 != null) {
			ConditionsInBracketContext conditionsInBracketCtx_1 = context.getChild(ConditionsInBracketContext.class, 1);
			if (conditionsInBracketCtx_1 != null) {
				// rule: conditions_in_bracket operator conditions_in_bracket
				ConditionTree conditionTree1 = parseConditionsInBracket(conditionsInBracketCtx_0);
				ConditionTree conditionTree2 = parseConditionsInBracket(conditionsInBracketCtx_1);

				String operator = context.getChild(OperatorContext.class, 0).getText();

				conditionTree = ConditionTree.getInstance(operator, conditionTree1, conditionTree2);
			} else {
				// rule: conditions_in_bracket
				ConditionTree conditionTree1 = parseConditionsInBracket(conditionsInBracketCtx_0);

				conditionTree = ConditionTree.getInstance(conditionTree1);
			}

		} else {
			ConditionContext conditionCtx_0 = context.getChild(ConditionContext.class, 0);

			if (conditionsCtx_0 != null && conditionsInBracketCtx_0 == null) {
				// rule: condition operator conditions

				Condition conditionTree1 = parseCondition(conditionCtx_0);
				ConditionTree conditionTree2 = parseConditions(conditionsCtx_0);

				String operator = context.getChild(OperatorContext.class, 0).getText();

				conditionTree = ConditionTree.getInstance(operator, conditionTree1, conditionTree2);
			} else {
				// rule: condition
				Condition conditionTree1 = parseCondition(conditionCtx_0);

				conditionTree = ConditionTree.getInstance(conditionTree1);
			}
		}

		return conditionTree;
	}

	private ConditionTree parseConditionsInBracket(ConditionsInBracketContext context) {
		// rule: '(' condition operator conditions ')'
		ConditionContext conditionCtx_0 = context.getChild(ConditionContext.class, 0);

		ConditionsContext conditionsCtx_0 = context.getChild(ConditionsContext.class, 0);

		Condition conditionTree1 = null;

		if (conditionCtx_0 != null) {
			conditionTree1 = parseCondition(conditionCtx_0);
		} else {
			ConditionsContext conditionsCtx_1 = context.getChild(ConditionsContext.class, 1);
			conditionTree1 = parseConditions(conditionsCtx_1);
		}
		ConditionTree conditionTree2 = parseConditions(conditionsCtx_0);

		String operator = context.getChild(OperatorContext.class, 0).getText();

		ConditionTree conditionTree = ConditionTree.getInstance(operator, conditionTree1, conditionTree2);

		return conditionTree;
	}

	// changed from enterCondition to parseCondition
	/* @Override */
	public Condition parseCondition(ConditionContext context) {
		// rule: condition: column conditional_operator STRING_VALUE | column
		// conditional_operator NUMBER
		String cf_cn = context.getChild(0).getText();
		String op = context.getChild(1).getText();
		String value = context.getChild(2).getText();
		String cf = "";
		String cn = "";
		if (cf_cn.indexOf(".") != -1) {
			// column family is there
			String[] cfC = cf_cn.split("\\.");
			cf = cfC[0];
			cn = cfC[1];
		} else {
			cn = cf_cn;
		}

		Condition condition = new Condition();

		condition.setColumnFamily(cf);
		condition.setColumnName(cn);
		condition.setConditionalOperator(op);
		//quote character can be " or ' . Removing quote character from first and last position
		condition.setValue(value.substring(1, value.length()-1));

		// below line is kept to support design of WideExplorer
		sqlBean.getConditionList().add(condition);

		return condition;

	}

}

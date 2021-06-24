/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqlBkDataSumAggFunction extends SqlAggFunction {

	public SqlBkDataSumAggFunction() {
		super("BKDATA_SUM", (SqlIdentifier)null, SqlKind.OTHER_FUNCTION, AGG_BKDATA_SUM,
			(SqlOperandTypeInference)null, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC,
			false, false);
	}

	private static final SqlReturnTypeInference AGG_BKDATA_SUM = (opBinding) -> {
		RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
		RelDataType type = typeFactory.getTypeSystem().deriveSumType(typeFactory, opBinding.getOperandType(0));
		SqlTypeName sqlTypeName = type.getSqlTypeName();
		switch (sqlTypeName) {
			case SMALLINT:
			case TINYINT:
			case INTEGER:
			case BIGINT:
				type =  ReturnTypes.BIGINT.inferReturnType(opBinding);
				break;
			case FLOAT:
			case DOUBLE:
			case DECIMAL:
				type =  ReturnTypes.DOUBLE.inferReturnType(opBinding);
				break;
			default:
				throw new IllegalArgumentException("bkdata_sum does not support sql type " + sqlTypeName.getName());
		}
		type = typeFactory.getTypeSystem().deriveSumType(typeFactory, type);
		return type;
	};


}

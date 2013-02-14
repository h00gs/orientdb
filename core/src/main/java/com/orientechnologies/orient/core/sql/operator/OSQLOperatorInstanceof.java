/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * INSTANCEOF operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLOperatorInstanceof extends OSQLOperator {

  public static final String NAME = "INSTANCEOF";

  public OSQLOperatorInstanceof() {
    super(NAME);
  }
  
  public OSQLOperatorInstanceof(OExpression left, OExpression right) {
		super(NAME, left, right);
	}

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {

		final OSchema schema = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema();

		final String baseClassName = String.valueOf(getRight().evaluate(context, candidate));
		final OClass baseClass = schema.getClass(baseClassName);
		if (baseClass == null){
			throw new OCommandExecutionException("Class '" + baseClassName + "' is not defined in database schema");
    }
    
    Object iLeft = getLeft().evaluate(context, candidate);
    
		OClass cls = null;
		if (iLeft instanceof OIdentifiable) {
			// GET THE RECORD'S CLASS
			final ORecord<?> record = ((OIdentifiable) iLeft).getRecord();
			if (record instanceof ODocument) {
				cls = ((ODocument) record).getSchemaClass();
			}
		} else if (iLeft instanceof String)
			// GET THE CLASS BY NAME
			cls = schema.getClass((String) iLeft);

		return cls != null ? cls.isSubClassOf(baseClass) : false;
	}

  @Override
  public OSQLOperatorInstanceof copy() {
    final OSQLOperatorInstanceof cp = new OSQLOperatorInstanceof();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }
}

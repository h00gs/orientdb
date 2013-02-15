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

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * CONTAINS VALUE operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLOperatorContainsValue extends OSQLOperator {

  public static final String NAME = "CONTAINSVALUE";

  public OSQLOperatorContainsValue() {
    super(NAME);
  }
  
  public OSQLOperatorContainsValue(OExpression left, OExpression right) {
    super(NAME, left, right);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    throw new UnsupportedOperationException("Not implemented yet.");
//    Object iLeft = getLeft().evaluate(context, candidate);
//    Object iRight = getRight().evaluate(context, candidate);
//    
//    if (iLeft instanceof Map<?, ?>) {
//      final Map<String, ?> map = (Map<String, ?>) iLeft;
//
//      if (condition != null) {
//        // CHECK AGAINST A CONDITION
//        for (Object o : map.values()) {
//          o = loadIfNeed(o);
//          if ((Boolean) condition.evaluate((ORecordSchemaAware<?>) o, null, iContext))
//            return true;
//        }
//      } else
//        return map.containsValue(iRight);
//
//    }
//    return false;
  }

  @Override
  public OSQLOperatorContainsValue copy() {
    final OSQLOperatorContainsValue cp = new OSQLOperatorContainsValue();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}

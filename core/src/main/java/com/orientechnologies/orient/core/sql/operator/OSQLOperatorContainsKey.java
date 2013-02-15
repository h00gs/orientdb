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
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * CONTAINS KEY operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLOperatorContainsKey extends OSQLOperator {

  public static final String NAME = "CONTAINSKEY";
  
  public OSQLOperatorContainsKey() {
    super(NAME);
  }
  
  public OSQLOperatorContainsKey(OExpression left, OExpression right) {
    super(NAME, left, right);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {    
    throw new UnsupportedOperationException("Not supported yet.");

//    if (iLeft instanceof Map<?, ?>) {
//
//      final Map<String, ?> map = (Map<String, ?>) iLeft;
//      return map.containsKey(iRight);
//    } else if (iRight instanceof Map<?, ?>) {
//
//      final Map<String, ?> map = (Map<String, ?>) iRight;
//      return map.containsKey(iLeft);
//    }
//    return false;
  }

  @Override
  public OSQLOperatorContainsKey copy() {
    final OSQLOperatorContainsKey cp = new OSQLOperatorContainsKey();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

  
}

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
package com.orientechnologies.orient.core.sql.method.misc;


import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OExpression;

import java.util.Map;

/**
 * CONTAINS KEY operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLMethodContainsKey extends OSQLMethod {

  public static final String NAME = "containskey";
  
  public OSQLMethodContainsKey() {
    super(NAME,1);
  }
  
  public OSQLMethodContainsKey(OExpression left, OExpression right) {
    super(NAME,1);
    children.add(left);
    children.add(right);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Object iLeft = children.get(0).evaluate(context,candidate);
    final Object iRight = children.get(1).evaluate(context,candidate);

    if (iLeft instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iLeft;
      return map.containsKey(iRight);
    }
    return false;
  }

  @Override
  public OSQLMethodContainsKey copy() {
    final OSQLMethodContainsKey cp = new OSQLMethodContainsKey();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

  
}

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
package com.orientechnologies.orient.core.sql.functions.coll;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Extract the last item of multi values (arrays, collections and maps) or return the same value for non multi-value types.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionLast extends OSQLFunctionAbstract {
  public static final String NAME = "last";
  private Object last;

  public OSQLFunctionLast() {
    super(NAME, 1, 1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    Object value = children.get(0).evaluate(context, candidate);

    if (OMultiValue.isMultiValue(value)){
      value = OMultiValue.getLastValue(value);
    }

    last = value;
    return value;
  }

  public String getSyntax() {
    return "Syntax error: last(<field>)";
  }

  @Override
  public OSQLFunctionLast copy() {
    final OSQLFunctionLast fct = new OSQLFunctionLast();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

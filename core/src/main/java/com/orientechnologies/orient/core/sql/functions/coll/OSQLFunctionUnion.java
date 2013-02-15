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

import java.util.Set;
import java.util.HashSet;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns, a
 * UNION of the collections received as parameters. Works also with no collection values.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionUnion extends OSQLFunctionAbstract {
  public static final String NAME = "union";

  private final Set<Object> result = new HashSet<Object>();
  
  public OSQLFunctionUnion() {
    super(NAME, 1, -1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    
    if (children.size() == 1) {
      // AGGREGATION MODE (STATEFULL)
      Object value = children.get(0).evaluate(context, candidate);
      if (value != null) {
        OMultiValue.add(result, value);
      }

      return result;
    } else {
      // IN-LINE MODE (STATELESS)
      final Set<Object> result = new HashSet<Object>();
      for (OExpression  exp : children) {
        Object value = exp.evaluate(context, candidate);
        if (value != null) {
          OMultiValue.add(result, value);
        }
      }
      return result;
    }
  }

  public String getSyntax() {
    return "Syntax error: union(<field>*)";
  }

  @Override
  public OSQLFunctionUnion copy() {
    final OSQLFunctionUnion fct = new OSQLFunctionUnion();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

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
package com.orientechnologies.orient.core.sql.functions.math;

import java.math.BigDecimal;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * Compute the average value for a field. Uses the context to save the last average number. When different Number class are used,
 * take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionAverage extends OSQLFunctionAbstract {
  public static final String NAME  = "avg";

  private Number sum;
  private int total = 0;

  public OSQLFunctionAverage() {
    super(NAME, 1, -1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    if (children.size() == 1) {
      final Object val = children.get(0).evaluate(context, candidate);
      
      if (val instanceof Number) {
        sum((Number)val);
      } else if (OMultiValue.isMultiValue(val)) {
        for (Object n : OMultiValue.getMultiValueIterable(val)) {
          sum((Number) n);
        }
      }

    } else {
      sum = null;
      for(OExpression  exp : children){
        final Object val = exp.evaluate(context, candidate);
        sum((Number)val);
      }
    }

    return getResult();
  }
  
  protected void sum(Number value) {
    if (value != null) {
      total++;
      if (sum == null)
        // FIRST TIME
        sum = value;
      else
        sum = OType.increment(sum, value);
    }
  }

  public String getSyntax() {
    return "Syntax error: avg(<field> [,<field>*])";
  }

  public Object getResult() {
      if (sum instanceof Integer) {
      return sum.intValue() / total;
    } else if (sum instanceof Long) {
      return sum.longValue() / total;
    } else if (sum instanceof Float) {
      return sum.floatValue() / total;
    } else if (sum instanceof Double) {
      return sum.doubleValue() / total;
    } else if (sum instanceof BigDecimal) {
      return ((BigDecimal) sum).divide(new BigDecimal(total));
    } else {
      return null;
    }
  }
  
  @Override
  public OSQLFunctionAverage copy() {
    final OSQLFunctionAverage fct = new OSQLFunctionAverage();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }

  
  
}

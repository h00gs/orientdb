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
package com.orientechnologies.orient.core.sql.functions.misc;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;

/**
 * Count the record that contains a field. Use * to indicate the record instead of the field. Uses the context to save the counter
 * number. When different Number class are used, take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionCount extends OSQLFunctionMathAbstract {
  public static final String NAME  = "count";

  private long               total = 0;

  public OSQLFunctionCount() {
    super(NAME, 1, 1);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    if (children.get(0) != null){
      total++;
    }
    return total;
  }
  
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public String getSyntax() {
    return "Syntax error: count(<field>|*)";
  }

  public Object getResult() {
    return total;
  }

  public void setResult(final Object iResult) {
    total = ((Number) iResult).longValue();
  }

  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    long total = 0;
    for (Object iParameter : resultsToMerge) {
      final long value = (Long) iParameter;
      total += value;
    }
    return total;
  }
  
  @Override
  public OSQLFunctionCount copy() {
    final OSQLFunctionCount fct = new OSQLFunctionCount();
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

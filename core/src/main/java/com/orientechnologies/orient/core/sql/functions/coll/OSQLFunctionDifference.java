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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the DIFFERENCE between the collections received as parameters. Works also with no collection values.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDifference extends OSQLFunctionAbstract {
  public static final String NAME = "difference";

  private Set<Object> result;
  private Set<Object> rejected;

  public OSQLFunctionDifference() {
    super(NAME, 1, -1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    
    Object value = children.get(0).evaluate(context, candidate);
    if (value == null){
      return null;
    }

    if (children.size() == 1) {
      // AGGREGATION MODE (STATEFULL)
      if (context == null) {
        result = new HashSet<Object>();
        rejected = new HashSet<Object>();
      }
      if (value instanceof Collection<?>) {
        addItemsToResult((Collection<Object>) value, result, rejected);
      } else {
        addItemToResult(value, result, rejected);
      }

      return null;
    } else {
      // IN-LINE MODE (STATELESS)
      final Set<Object> result = new HashSet<Object>((Collection<?>) value);
      final Set<Object> rejected = new HashSet<Object>();

      for (OExpression exp : children) {
        Object iParameter = children.get(0).evaluate(context, candidate);
        if (iParameter instanceof Collection<?>) {
          addItemsToResult((Collection<Object>) value, result, rejected);
        } else {
          addItemToResult(value, result, rejected);
        }
      }

      return result;
    }
  }

  private static void addItemToResult(Object o, Set<Object> accepted, Set<Object> rejected) {
    if (!accepted.contains(o) && !rejected.contains(o)) {
      accepted.add(o);
    } else {
      accepted.remove(o);
      rejected.add(o);
    }
  }

  private static void addItemsToResult(Collection<Object> co, Set<Object> accepted, Set<Object> rejected) {
    for (Object o : co) {
      addItemToResult(o, accepted, rejected);
    }
  }

  public String getSyntax() {
    return "Syntax error: difference(<field>*)";
  }

  @Override
  public OSQLFunctionDifference copy() {
    final OSQLFunctionDifference fct = new OSQLFunctionDifference();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

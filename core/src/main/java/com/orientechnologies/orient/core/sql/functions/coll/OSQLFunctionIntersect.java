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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * This operator can work as aggregate or inline. If only one argument is passed than aggregates, otherwise executes, and returns,
 * the INTERSECTION of the collections received as parameters.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionIntersect extends OSQLFunctionAbstract {
  public static final String NAME = "intersect";

  private Set result;
  
  public OSQLFunctionIntersect() {
    super(NAME, 1, -1);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    
    Object value = children.get(0).evaluate(context, candidate);

    if (value == null){
      return Collections.emptySet();
    }

    if (!(value instanceof Collection<?>)){
      value = Arrays.asList(value);
    }

    final Collection<?> coll = (Collection<?>) value;

    if (children.size() == 1) {
      // AGGREGATION MODE (STATEFULL)
      if (result == null) {
        // ADD ALL THE ITEMS OF THE FIRST COLLECTION
        result = new HashSet<Object>(coll);
      } else {
        // INTERSECT IT AGAINST THE CURRENT COLLECTION
        result.retainAll(coll);
      }
      return result;
    } else {
      // IN-LINE MODE (STATELESS)
      final HashSet<Object> result = new HashSet<Object>(coll);

      for (int i=1,n=children.size(); i<n; ++i) {
        value = children.get(i).evaluate(context, candidate);

        if (value != null) {
          if (!(value instanceof Collection<?>)){
            // CONVERT IT INTO A COLLECTION
            value = Arrays.asList(value);
          }

          result.retainAll((Collection<?>) value);
        } else
          result.clear();
      }

      return result;
    }
  }

  public String getSyntax() {
    return "Syntax error: intersect(<field>*)";
  }

  @Override
  public OSQLFunctionIntersect copy() {
    final OSQLFunctionIntersect fct = new OSQLFunctionIntersect();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

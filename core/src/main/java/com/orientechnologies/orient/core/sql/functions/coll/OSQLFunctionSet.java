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
 * This operator add an item in a set. The set doesn't accept duplicates, so adding multiple times the same value has no effect: the
 * value is contained only once.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionSet extends OSQLFunctionAbstract {
  public static final String NAME = "set";

  private final Set<Object> result = new HashSet<Object>();
  
  public OSQLFunctionSet() {
    super(NAME, 1, -1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    
    final Set<Object> result;
    if(children.size() == 1){
      // AGGREGATION MODE (STATEFULL)
      result = this.result;
    }else{
      // IN-LINE MODE (STATELESS)
      result = new HashSet<Object>();
    }
    
    for (OExpression exp : children) {
      final Object value = exp.evaluate(context, candidate);
      if (value != null) {
        if (value instanceof Collection<?>){
          // INSERT EVERY SINGLE COLLECTION ITEM
          result.addAll((Collection<?>) value);
        }else{
          result.add(value);
        }
      }
    }

    return result;
  }

  public String getSyntax() {
    return "Syntax error: set(<value>*)";
  }

  @Override
  public OSQLFunctionSet copy() {
    final OSQLFunctionSet fct = new OSQLFunctionSet();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

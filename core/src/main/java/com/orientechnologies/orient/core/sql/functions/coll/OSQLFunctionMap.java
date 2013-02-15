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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * This operator add an entry in a map. The entry is composed by a key and a value.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionMap extends OSQLFunctionAbstract {
  public static final String NAME = "map";

  private final Map result = new HashMap();
  
  public OSQLFunctionMap() {
    super(NAME, 1, -1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {

    final Map result;
    if(children.size()>2){
      // IN LINE MODE
      result = new HashMap();
    }else{
      // AGGREGATION MODE (STATEFULL)
      result = this.result;
    }
    
    if(children.size() == 1){
      final Object val = children.get(0).evaluate(context, candidate);
      if(val instanceof Map){
        result.putAll((Map)val);
      }else{
        throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
      }
    } else if (children.size() % 2 != 0){
      throw new IllegalArgumentException("Map function: expected a map or pairs of parameters as key, value");
    } else {
      for (int i=0,n=children.size(); i<n; i += 2) {
        final Object key = children.get(i).evaluate(context, candidate);
        final Object value = children.get(i+1).evaluate(context, candidate);
        if (value != null) {
          result.put(key, value);
        }
      }
    }
    
    return result;
  }

  public String getSyntax() {
    return "Syntax error: map(<map>|[<key>,<value>]*)";
  }

  @Override
  public OSQLFunctionMap copy() {
    final OSQLFunctionMap fct = new OSQLFunctionMap();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

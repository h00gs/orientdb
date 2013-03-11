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
import com.orientechnologies.orient.core.sql.model.OCollection;
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OSearchContext;
import com.orientechnologies.orient.core.sql.model.OSearchResult;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;

/**
 * CONTAINS ALL operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLMethodContainsAll extends OSQLMethod {
  
  public static final String NAME = "containsall";

  public OSQLMethodContainsAll() {
    super(NAME,1);
  }

  public OSQLMethodContainsAll(OExpression left, OExpression right) {
    super(NAME,1);
    children.add(left);
    children.add(right);
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
      
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {

    final Object iLeft = children.get(0).evaluate(context,candidate);    
    final OExpression filter = children.get(1);

    if (filter instanceof OLiteral || filter instanceof OCollection) {
      final Object iRight = children.get(1).evaluate(context, candidate);
      final Object[] leftvalues = toArray(iLeft);
      final Object[] rightValues = toArray(iRight);

      int matches = 0;
      for (final Object l : leftvalues) {
        for (final Object r : rightValues) {
          if (OEquals.equals(l, r)) {
            ++matches;
            break;
          }
        }
      }
      
      return matches == rightValues.length;
      
    } else {
      //it's not a real contain, it's a filter test
      if (iLeft instanceof Iterable<?>) {
        final Iterable<Object> iterable = (Iterable<Object>) iLeft;

        for (final Object o : iterable) {
          if (!Boolean.TRUE.equals(filter.evaluate(context, o))) {
            return false;
          }
        }        
        return true;
        
      } else {
        return Boolean.TRUE.equals(filter.evaluate(context, iLeft));
      }
    }

  }

  private Object[] toArray(Object candidate){
      if(candidate == null){
          return new Object[0];
      }else if(candidate.getClass().isArray()){
          final Object[] objs = new Object[Array.getLength(candidate)];
          for(int i=0;i<objs.length;i++){
              objs[i] = Array.get(candidate,i);
          }
          return objs;
      }else if(candidate instanceof Collection){
        return ((Collection)candidate).toArray();
      }else{
          return new Object[]{candidate};
      }
  }

  @Override
  public OSQLMethodContainsAll copy() {
    final OSQLMethodContainsAll cp = new OSQLMethodContainsAll();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}

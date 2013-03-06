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
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;

/**
 * CONTAINS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLMethodContains extends OSQLMethod {

  public static final String NAME = "contains";

  public OSQLMethodContains() {
    super(NAME,1);
  }
  
  public OSQLMethodContains(OExpression left, OExpression right) {
    super(NAME,1);
    children.add(left);
    children.add(right);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {

    final Object iLeft = children.get(0).evaluate(context,candidate);    
    final OExpression filter = children.get(1);

      if (filter instanceof OLiteral) {
          final Object iRight = filter.evaluate(context, candidate);
          if (iLeft instanceof Iterable<?>) {
              final Iterable<Object> iterable = (Iterable<Object>) iLeft;

              // CHECK AGAINST A SINGLE VALUE
              for (final Object o : iterable) {
                  if (OEquals.equals(o,iRight)) {
                      return true;
                  }
              }
          } else {
              return OEquals.equals(iLeft,iRight);
          }
      }else{
        //it's not a real contain, it's a filter test
        if (iLeft instanceof Iterable<?>) {
          final Iterable<Object> iterable = (Iterable<Object>) iLeft;

          // CHECK AGAINST A SINGLE VALUE
          for (final Object o : iterable) {
            if(Boolean.TRUE.equals(filter.evaluate(context, o))){
                return true;
            }
          }
        }else{
            return Boolean.TRUE.equals(filter.evaluate(context, iLeft));
        }
      }
    return false;
  }

  @Override
  public OSQLMethodContains copy() {
    final OSQLMethodContains cp = new OSQLMethodContains();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}

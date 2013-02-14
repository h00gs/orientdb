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
package com.orientechnologies.orient.core.sql.operator;


import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * CONTAINS operator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLOperatorContains extends OSQLOperator {

  public static final String NAME = "CONTAINS";

  public OSQLOperatorContains() {
    super(NAME);
  }
  
  public OSQLOperatorContains(OExpression left, OExpression right) {
    super(NAME,left,right);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    throw new UnsupportedOperationException("Not supported yet.");
    
//    final OSQLFilterCondition condition;
//    if (iCondition.getLeft() instanceof OSQLFilterCondition)
//      condition = (OSQLFilterCondition) iCondition.getLeft();
//    else if (iCondition.getRight() instanceof OSQLFilterCondition)
//      condition = (OSQLFilterCondition) iCondition.getRight();
//    else
//      condition = null;
//
//    if (iLeft instanceof Iterable<?>) {
//
//      final Iterable<Object> iterable = (Iterable<Object>) iLeft;
//
//      if (condition != null) {
//        // CHECK AGAINST A CONDITION
//        for (final Object o : iterable) {
//          final OIdentifiable id;
//          if (o instanceof OIdentifiable)
//            id = (OIdentifiable) o;
//          else if (o instanceof Map<?, ?>) {
//            final Iterator<OIdentifiable> iter = ((Map<?, OIdentifiable>) o).values().iterator();
//            id = iter.hasNext() ? iter.next() : null;
//          } else if (o instanceof Iterable<?>) {
//            final Iterator<OIdentifiable> iter = ((Iterable<OIdentifiable>) o).iterator();
//            id = iter.hasNext() ? iter.next() : null;
//          } else
//            continue;
//
//          if ((Boolean) condition.evaluate(id, null, iContext) == Boolean.TRUE)
//            return true;
//        }
//      } else {
//        // CHECK AGAINST A SINGLE VALUE
//        for (final Object o : iterable) {
//          if (OQueryOperatorEquals.equals(iRight, o))
//            return true;
//        }
//      }
//    } else if (iRight instanceof Iterable<?>) {
//
//      // CHECK AGAINST A CONDITION
//      final Iterable<OIdentifiable> iterable = (Iterable<OIdentifiable>) iRight;
//
//      if (condition != null) {
//        for (final OIdentifiable o : iterable) {
//          if ((Boolean) condition.evaluate(o, null, iContext) == Boolean.TRUE)
//            return true;
//        }
//      } else {
//        // CHECK AGAINST A SINGLE VALUE
//        for (final Object o : iterable) {
//          if (OQueryOperatorEquals.equals(iLeft, o))
//            return true;
//        }
//      }
//    }
//    return false;
  }

  @Override
  public OSQLOperatorContains copy() {
    final OSQLOperatorContains cp = new OSQLOperatorContains();
    cp.getArguments().addAll(getArguments());
    cp.setAlias(getAlias());
    return cp;
  }

}

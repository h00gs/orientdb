/*
 * Copyright 2013 Orient Technologies.
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.model;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OLike extends OExpressionWithChildren{
  
  private static final double EPS = 1E-12;

  public OLike(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OLike(String alias, OExpression left, OExpression right) {
    super(alias,left,right);
  }
  
  public OExpression getLeft(){
    return children.get(0);
  }
  
  public OExpression getRight(){
    return children.get(1);
  }
  
  @Override
  protected String thisToString() {
    return "(Like)";
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    return equals(getLeft(), getRight(), context, candidate);
  }

  static boolean equals(OExpression left, OExpression right, OCommandContext context, Object candidate){
    final Object value1 = left.evaluate(context, candidate);
    final Object value2 = right.evaluate(context, candidate);
    
    if (value1 == value2) {
      // Includes the (value1 == null && value2 == null) case.
      return true;
    }
    if (value1 == null || value2 == null) {
      // No need to check for (value2 != null) or (value1 != null).
      // If they were null, the previous check would have caugh them.
      return false;
    }

    //resolving for numbers
    if (value1 instanceof Number && value2 instanceof Number) {
      //test number case
      return numberEqual((Number) value1, (Number) value2);
    } else if (value1.equals(value2)) {
      //test standard equal
      //but classes are not the same, so will have to use the converters
      //to ensure a proper compare
      return true;
    }
    
    return false;
  }
  
  private static boolean numberEqual(final Number value1, final Number value2) {
    final Number n1 = (Number) value1;
    final Number n2 = (Number) value2;

    if (   (n1 instanceof Float) || (n1 instanceof Double)
        || (n2 instanceof Float) || (n2 instanceof Double)) {
      final double d1 = n1.doubleValue();
      final double d2 = n2.doubleValue();
      if (Double.doubleToLongBits(d1) == Double.doubleToLongBits(d2)) {
        return true;
      }
      if (Math.abs(d1 - d2) < EPS * Math.max(Math.abs(d1), Math.abs(d2))) {
        return true;
      }
    } else {
      return n1.longValue() == n2.longValue();
    }
    return false;
  }

  @Override
  public OIndexResult searchIndex(OClass clazz, OSortBy[] sorts) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return super.equals(obj);
  }
  
  @Override
  public OLike copy() {
    return new OLike(alias, getLeft(), getRight());
  }
  
}

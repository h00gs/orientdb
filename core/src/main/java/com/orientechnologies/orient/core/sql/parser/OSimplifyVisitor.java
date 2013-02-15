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
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OAnd;
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.ONotEquals;
import com.orientechnologies.orient.core.sql.model.OOperatorDivide;
import com.orientechnologies.orient.core.sql.model.OOperatorMinus;
import com.orientechnologies.orient.core.sql.model.OOperatorModulo;
import com.orientechnologies.orient.core.sql.model.OOperatorMultiply;
import com.orientechnologies.orient.core.sql.model.OOperatorPlus;
import com.orientechnologies.orient.core.sql.model.OOperatorPower;
import com.orientechnologies.orient.core.sql.model.OOr;
import com.orientechnologies.orient.core.sql.operator.OSQLOperator;

/**
 * Simplify expressions.
 * @author Johann Sorel (Geomatys)
 */
public class OSimplifyVisitor extends OCopyVisitor{
    
  public static final OSimplifyVisitor INSTANCE = new OSimplifyVisitor();
  
  private OSimplifyVisitor() {}

  @Override
  public Object visit(OAnd candidate, Object extraData) {
    candidate = (OAnd) super.visit(candidate, extraData);

    final OExpression left = candidate.getLeft();
    final OExpression right = candidate.getRight();

    boolean leftIsAlwaysTrue = false;
    if(left == OExpression.INCLUDE){
      leftIsAlwaysTrue = true;
    }else if(left instanceof OLiteral){
      final Object value = ((OLiteral) left).getValue();
      if (Boolean.TRUE.equals(value)) {
        leftIsAlwaysTrue = true;
      }else if (Boolean.FALSE.equals(value)) {
        return OExpression.EXCLUDE;
      } else if (value == null) {
        return OExpression.EXCLUDE;
      }
    }
    
    boolean rightIsAlwaysTrue = false;
    if(right == OExpression.INCLUDE){
      rightIsAlwaysTrue = true;
    }else if(right instanceof OLiteral){
      final Object value = ((OLiteral) right).getValue();
      if (Boolean.TRUE.equals(value)) {
        rightIsAlwaysTrue = true;
      }else if (Boolean.FALSE.equals(value)) {
        return OExpression.EXCLUDE;
      } else if (value == null) {
        return OExpression.EXCLUDE;
      }
    }
    
    if(leftIsAlwaysTrue && rightIsAlwaysTrue){
      return OExpression.INCLUDE;
    }else if(leftIsAlwaysTrue){
      return right;
    }else if(rightIsAlwaysTrue){
      return left;
    }
    
    return candidate;
  }

  @Override
  public Object visit(OOr candidate, Object extraData) {
    candidate = (OOr) super.visit(candidate, extraData);

    OExpression left = candidate.getLeft();
    OExpression right = candidate.getRight();
    
    if (left == OExpression.INCLUDE || right == OExpression.INCLUDE) {
      return OExpression.INCLUDE;
    }

    if (left instanceof OLiteral) {
      final Object value = ((OLiteral) left).getValue();
      if (Boolean.TRUE.equals(value)) {
        return OExpression.INCLUDE;
      }else if (value == null || Boolean.FALSE.equals(value)) {
        left = OExpression.EXCLUDE;
      }
    }
    
    if (right instanceof OLiteral) {
      final Object value = ((OLiteral) right).getValue();
      if (Boolean.TRUE.equals(value)) {
        return OExpression.INCLUDE;
      }else if (value == null || Boolean.FALSE.equals(value)) {
        right = OExpression.EXCLUDE;
      }
    }
    
    if (left == OExpression.EXCLUDE) {
      return right;
    }else if (right == OExpression.EXCLUDE) {
      return left;
    }
    
    return candidate;
  }

  @Override
  public Object visit(OOperatorDivide candidate, Object data) {
    candidate = (OOperatorDivide) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OOperatorMinus candidate, Object data) {
    candidate = (OOperatorMinus) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OOperatorModulo candidate, Object data) {
    candidate = (OOperatorModulo) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OOperatorMultiply candidate, Object data) {
    candidate = (OOperatorMultiply) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OOperatorPlus candidate, Object data) {
    candidate = (OOperatorPlus) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OOperatorPower candidate, Object data) {
    candidate = (OOperatorPower) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OSQLFunction candidate, Object data) {
    candidate = (OSQLFunction) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OSQLMethod candidate, Object data) {
    candidate = (OSQLMethod) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OSQLOperator candidate, Object data) {
    candidate = (OSQLOperator) super.visit(candidate, data);
    if(candidate.isStatic()){
      //we can preevaluate this one
      return new OLiteral(candidate.evaluate(null, null));
    }
    return candidate;
  }
  
  @Override
  public Object visit(OEquals candidate, Object extraData) {
    candidate = (OEquals) super.visit(candidate, extraData);

    //case : 15 = 15
    if (   candidate.getLeft() instanceof OLiteral
        && candidate.getRight() instanceof OLiteral) {
      //we can preevaluate this one
      if(Boolean.TRUE.equals(candidate.evaluate(null, null))){
        return OExpression.INCLUDE;
      }else{
        return OExpression.EXCLUDE;
      }
    }
    
    return candidate;
  }
  
  @Override
  public Object visit(ONotEquals candidate, Object extraData) {
    candidate = (ONotEquals) super.visit(candidate, extraData);

    //case : 15 != 16
    if (   candidate.getLeft() instanceof OLiteral
        && candidate.getRight() instanceof OLiteral) {
      //we can preevaluate this one
      if(Boolean.TRUE.equals(candidate.evaluate(null, null))){
        return OExpression.INCLUDE;
      }else{
        return OExpression.EXCLUDE;
      }
    }
    
    return candidate;
  }
  
  
}

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    final List<OExpression> children = candidate.getChildren();
    final List<OExpression> newChildren = new ArrayList<OExpression>(children.size());

    for (OExpression child : children) {
      final OExpression cloned = (OExpression) child.accept(this, extraData);

      // if one element is Exclude, the all chain is Exclude
      if (cloned == OExpression.EXCLUDE) {
        return OExpression.EXCLUDE;
      }

      // we can skip includes
      if (cloned == OExpression.INCLUDE) {
        continue;
      }

      if (cloned instanceof OAnd) {
        //we can append sub children here
        newChildren.addAll(((OAnd) cloned).getChildren());
        continue;
      }

      newChildren.add(cloned);
    }

    // all elements have been simplified
    if (newChildren.isEmpty()) {
      return OExpression.INCLUDE;
    }

    // only one element, unwrap it
    if (newChildren.size() == 1) {
      return newChildren.get(0);
    }

    return new OAnd(candidate.getAlias(),newChildren);
  }

  @Override
  public Object visit(OOr candidate, Object extraData) {
    candidate = (OOr) super.visit(candidate, extraData);

    final List<OExpression> children = candidate.getChildren();
    final List<OExpression> newChildren = new ArrayList<OExpression>(children.size());


    for (OExpression child : children) {
      final OExpression cloned = (OExpression) child.accept(this, extraData);

      // if one element is Include, the all chain is Include
      if (cloned == OExpression.INCLUDE) {
        return OExpression.INCLUDE;
      }

      // we can skip excludes
      if (cloned == OExpression.EXCLUDE) {
        continue;
      }

      if (cloned instanceof OOr) {
        //we can append sub children here
        newChildren.addAll(((OOr) cloned).getChildren());
        continue;
      }
      
      newChildren.add(cloned);
    }

    // we might end up with an empty list
    if (newChildren.isEmpty()) {
      return OExpression.EXCLUDE;
    }

    // remove the logic we have only one filter
    if (newChildren.size() == 1) {
      return newChildren.get(0);
    }

    // else return the cloned and simplified up list
    return new OOr(candidate.getAlias(),children);
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

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
public class OOr extends OExpressionWithChildren{

  public OOr(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OOr(String alias, OExpression left, OExpression right) {
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
    return "(Or)";
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    final Object objLeft = children.get(0).evaluate(context, candidate);
    if(!(objLeft instanceof Boolean)){
      //can not combine non boolean values
      return false;
    }else if(((Boolean)objLeft)){
      //no need to evaluate the right side
      return true;
    }
    final Object objRight = children.get(1).evaluate(context, candidate);
    if(!(objRight instanceof Boolean)){
      //can not combine non boolean values
      return false;
    }else if(((Boolean)objRight)){
      return true;
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
  public OOr copy() {
    return new OOr(alias,getLeft(),getRight());
  }
  
}

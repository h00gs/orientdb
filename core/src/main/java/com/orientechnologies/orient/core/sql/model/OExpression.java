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
public interface OExpression {
  
  public static final OExpression INCLUDE = new Constant(true);
  public static final OExpression EXCLUDE = new Constant(false);
  
  Object evaluate(OCommandContext context, Object candidate);
  
  String getAlias();
  
  void setAlias(String alias);
  
  boolean isStatic();
  
  boolean isContextFree();
  
  boolean isDocumentFree();
  
  OIndexResult searchIndex(OClass clazz ,OSortBy[] sorts);
  
  Object accept(OExpressionVisitor visitor, Object data);
  
  OExpression copy();
  
  public static final class Constant extends OExpressionAbstract{

    private final boolean value;

    public Constant(boolean value) {
      this.value = value;
    }
    
    @Override
    protected String thisToString() {
      if(value){
        return "INCLUDE";
      }else{
        return "EXCLUDE";
      }
    }

    @Override
    public Object evaluate(OCommandContext context, Object candidate) {
      return Boolean.TRUE;
    }

    @Override
    public boolean isContextFree() {
      return true;
    }

    @Override
    public boolean isDocumentFree() {
      return true;
    }

    @Override
    public OIndexResult searchIndex(OClass clazz, OSortBy[] sorts) {
      return null;
    }

    @Override
    public Object accept(OExpressionVisitor visitor, Object data) {
      if(value){
        return visitor.visitInclude(this, data);
      }else{
        return visitor.visitExclude(this, data);
      }
    }

    @Override
    public OExpression copy() {
      //immutable
      return this;
    }
    
  }
  
}

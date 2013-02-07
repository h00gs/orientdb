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
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OFunction extends OExpressionWithChildren {
  
  private final String name;
  
  public OFunction(String name, List<OExpression> arguments){
    this(name,null,arguments);
  }
  
  public OFunction(String name, String alias, List<OExpression> arguments){
    super(alias,arguments);
    this.name = name;
  }
  
  public String getName(){
    return name;
  }
  
  public List<OExpression> getArguments(){
    return children;
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    throw new UnsupportedOperationException("Not supported yet.");
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
  protected String thisToString() {
    return "(Function) "+name;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + (this.name != null ? this.name.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final OFunction other = (OFunction) obj;
    if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
      return false;
    }
    return super.equals(obj);
  }
  
  @Override
  public OFunction copy() {
    return new OFunction(alias, getArguments());
  }
  
}

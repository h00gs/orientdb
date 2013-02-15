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

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OUnset extends OExpressionAbstract{

  private final String name;

  public OUnset() {
    this(null);
  }

  public OUnset(String name) {
    this.name = name;
  }

  public String getParameterName() {
    return name;
  }
  
  @Override
  protected String thisToString() {
    return "(Unset) " + ((name==null)? "?" : name);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    throw new UnsupportedOperationException("Unset expression must be resolved before evaluation or index search.");
  }

  @Override
  public boolean isContextFree() {
    return false;
  }

  @Override
  public boolean isDocumentFree() {
    return true;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public int hashCode() {
    int hash = 5;
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
    final OUnset other = (OUnset) obj;
    return true;
  }
  
  @Override
  public OUnset copy() {
    return new OUnset(name);
  }
  
}

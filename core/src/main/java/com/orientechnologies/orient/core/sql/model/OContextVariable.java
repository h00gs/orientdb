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
public final class OContextVariable extends OExpressionAbstract {

  private final String varName;

  public OContextVariable(String name) {
    this(name,null);
  }
  
  public OContextVariable(String name, String alias) {
    super(alias);
    this.varName = name;
  }

  public Object getVariableName() {
    return varName;
  }
  
  @Override
  public Object evaluateNow(OCommandContext context, Object candidate) {
    return context.getVariable(varName);
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
  protected String thisToString() {
    return "(Variable) "+varName;
  }
  
  @Override
  public OContextVariable copy() {
    return new OContextVariable(varName,alias);
  }
  
}

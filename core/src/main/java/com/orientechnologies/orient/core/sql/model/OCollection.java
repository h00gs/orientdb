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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OCollection extends OExpressionAbstract {

  private final List<OExpression> children;

  public OCollection(List<OExpression> children) {
    this(null,children);
  }

  public OCollection(String alias, List<OExpression> children) {
    super(alias);
    this.children = new ArrayList<OExpression>(children);
  }

  public List<OExpression> getChildren() {
    return Collections.unmodifiableList(children);
  }
  
  @Override
  protected String thisToString() {
    final StringBuilder sb = new StringBuilder("(Collection) ");
    for(OExpression exp : children){
      sb.append(exp.toString());
    }
    return sb.toString();
  }

  @Override
  protected List evaluateNow(OCommandContext context, Object candidate) {
    final List value = new ArrayList();
    for(OExpression exp : children){
      value.add(exp.evaluate(context, candidate));
    }
    return value;
  }

  @Override
  public boolean isContextFree() {
    for(OExpression exp : children){
      if(!exp.isContextFree()){
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    for(OExpression exp : children){
      if(!exp.isDocumentFree()){
        return false;
      }
    }
    return true;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public OCollection copy() {
    return new OCollection(alias, children);
  }
  
}

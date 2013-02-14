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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class OSQLOperator extends OSQLFunctionAbstract {

  public OSQLOperator(String name) {
    super(name,2);
  }
  
  public OSQLOperator(String name, OExpression left, OExpression right) {
    super(name,2);
    getChildren().add(left);
    getChildren().add(right);
  }
  
  public OExpression getLeft(){
    return children.get(0);
  }
  
  public OExpression getRight(){
    return children.get(1);
  }
  
  @Override
  public String getSyntax() {
    return "<exp> "+getName()+" <exp>";
  }
  
  @Override
  protected String thisToString() {
    return "(Operator) "+getName();
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  @Override
  public abstract OSQLOperator copy();
  
}

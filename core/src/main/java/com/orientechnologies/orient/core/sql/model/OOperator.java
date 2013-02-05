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

import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OOperator extends OFunction{

  public OOperator(String name, List<OExpression> arguments) {
    this(name, null, arguments);
  }

  public OOperator(String name, String alias, List<OExpression> arguments) {
    super(name, alias, arguments);
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  @Override
  protected String thisToString() {
    return "(Operator) "+getName();
  }
  
}

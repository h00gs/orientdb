/*
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
package com.orientechnologies.orient.core.sql.method;

import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;
import com.orientechnologies.orient.core.sql.model.OFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Methods can be used on various objects with different number of arguments. SQL syntax : <object_name>.<method_name>([parameters])
 * 
 * @author Johann Sorel (Geomatys)
 */
public abstract class OSQLMethod extends OFunction implements Comparable<OSQLMethod> {

  public OSQLMethod(String name, List<OExpression> arguments) {
    super(name, arguments);
  }

  public OSQLMethod(String name, String alias, List<OExpression> arguments) {
    super(name, alias, arguments);
  }
  
  /**
   * Returns a convinient SQL String representation of the method.
   * <p>
   * Example :
   * 
   * <pre>
   *  field.myMethod( param1, param2, [optionalParam3])
   * </pre>
   * 
   * This text will be used in exception messages.
   * 
   * @return String , never null.
   */
  public abstract String getSyntax();

  /**
   * @return minimum number of arguments requiered by this method
   */
  public abstract int getMinParams();

  /**
   * @return maximum number of arguments requiered by this method
   */
  public abstract int getMaxParams();

  public OExpression getSource(){
    return getArguments().get(0);
  }
  
  public List<OExpression> getMethodArguments(){
    final List<OExpression> args = new ArrayList<OExpression>(getArguments());
    args.remove(0);
    return Collections.unmodifiableList(args);
  }
  
  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  @Override
  protected String thisToString() {
    return "(Method) "+getName();
  }

  @Override
  public abstract OSQLMethod copy();
  
}

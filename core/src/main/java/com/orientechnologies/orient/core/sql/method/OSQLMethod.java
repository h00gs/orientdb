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

import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Methods can be used on various objects with different number of arguments. SQL syntax : <object_name>.<method_name>([parameters])
 * 
 * @author Johann Sorel (Geomatys)
 */
public abstract class OSQLMethod extends OSQLFunctionAbstract {

  public OSQLMethod(String name) {
    this(name, 0);
  }

  public OSQLMethod(String name, int nbparams) {
    this(name, nbparams, nbparams);
  }
  
  public OSQLMethod(String name, int minParam, int maxParam) {
    //we add 1 argument for the source.
    super(name, minParam+1, maxParam+1);
  }
  
  /**
   * @return minimum number of arguments requiered by this method
   */
  public int getMethodMinParams(){
    return getMinParams()-1;
  }

  /**
   * @return maximum number of arguments requiered by this method
   */
  public int getMethodMaxParams(){
    return getMaxParams()-1;
  }

  public OExpression getSource(){
    return getArguments().get(0);
  }
  
  public List<OExpression> getMethodArguments(){
    final List<OExpression> args = new ArrayList<OExpression>(getArguments());
    args.remove(0);
    return Collections.unmodifiableList(args);
  }
  
  @Override
  public String getSyntax() {
    final int minparams = getMethodMinParams();
    final int maxparams = getMethodMinParams();
    final StringBuilder sb = new StringBuilder("<field>.");
    sb.append(getName());
    sb.append('(');
    for (int i = 0; i < minparams; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append("param");
      sb.append(i + 1);
    }
    if (minparams != maxparams) {
      sb.append('[');
      for (int i = minparams; i < maxparams; i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append("param");
        sb.append(i + 1);
      }
      sb.append(']');
    }
    sb.append(')');

    return sb.toString();
  }
  
  @Override
  protected String thisToString() {
    return "(Method) "+getName();
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  @Override
  public abstract OSQLMethod copy();
  
}

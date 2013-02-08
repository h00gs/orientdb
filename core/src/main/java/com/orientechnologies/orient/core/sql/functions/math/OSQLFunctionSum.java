/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.math;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.model.OExpression;

/**
 * Computes the sum of field. Uses the context to save the last sum number. When different Number class are used, take the class
 * with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionSum extends OSQLFunctionMathAbstract {
  public static final String NAME = "sum";

  private Number sum;

  public OSQLFunctionSum() {
    super(NAME, 1, -1);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    
    if (getArguments().size() == 1) {
      //group by case
      final Number value = (Number)children.get(0).evaluate(context, candidate);

      if (sum == null){
        sum = value;
      }else{
        sum = value.doubleValue() + sum.doubleValue();
      }

      return sum;
    } else {
      //sum of several elements
      Number sum = null;
      for(OExpression  ex : children){
        final Number value = (Number) ex.evaluate(context, candidate);
        if(sum == null){
          sum = value;
        }else{
          sum = value.doubleValue() + sum.doubleValue();
        }
      }
      return sum;
    }
  }
  
  public String getSyntax() {
    return "Syntax error: sum(<field> [,<field>*])";
  }

  @Override
  public OSQLFunctionSum copy() {
    final OSQLFunctionSum fct = new OSQLFunctionSum();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }
  
}

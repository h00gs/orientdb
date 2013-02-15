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
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Count the record that contains a field. Use * to indicate the record instead of the field. Uses the context to save the counter
 * number. When different Number class are used, take the class with most precision.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionCount extends OSQLFunctionAbstract {
  public static final String NAME  = "count";

  private long total = 0;

  public OSQLFunctionCount() {
    super(NAME, 1, 1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    total++;
    return total;
  }
  
  @Override
  public String getSyntax() {
    return "Syntax error: count(<field>|*)";
  }
  
  @Override
  public OSQLFunctionCount copy() {
    final OSQLFunctionCount fct = new OSQLFunctionCount();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

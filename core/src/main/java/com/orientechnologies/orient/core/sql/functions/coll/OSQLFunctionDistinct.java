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
package com.orientechnologies.orient.core.sql.functions.coll;

import java.util.LinkedHashSet;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Keeps items only once removing duplicates
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDistinct extends OSQLFunctionAbstract {
  public static final String NAME    = "distinct";

  private Set<Object> set = new LinkedHashSet<Object>();

  public OSQLFunctionDistinct() {
    super(NAME, 1, 1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Object value = children.get(0).evaluate(context, candidate);

    if (value != null && !set.contains(value)) {
      set.add(value);
    }

    return set;
  }

  public boolean filterResult() {
    return true;
  }

  public String getSyntax() {
    return "Syntax error: distinct(<field>)";
  }

  @Override
  public OSQLFunctionDistinct copy() {
    final OSQLFunctionDistinct fct = new OSQLFunctionDistinct();
    fct.setAlias(getAlias());
    fct.getArguments().addAll(getArguments());
    return fct;
  }

}

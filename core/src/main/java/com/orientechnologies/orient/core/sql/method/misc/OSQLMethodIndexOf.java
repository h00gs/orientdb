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
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.model.OExpression;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodIndexOf extends OAbstractSQLMethod {

  public static final String NAME = "indexof";

  public OSQLMethodIndexOf() {
    super(NAME, 1, 2);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    final List<OExpression> arguments = getMethodArguments();
    Object value = getSource().evaluate(context, candidate);
    final String param0 = arguments.get(0).evaluate(context, candidate).toString();
    if (arguments.size() > 1) {
      final Object param1 = arguments.get(1).evaluate(context, candidate);
      String toFind = param0.substring(1, param0.length() - 1);
      int startIndex = arguments.size() > 1 ? Integer.parseInt(param1.toString()) : 0;
      value = value != null ? value.toString().indexOf(toFind, startIndex) : null;
    }
    return value;
  }
  
  @Override
  public OSQLMethodIndexOf copy() {
    final OSQLMethodIndexOf method = new OSQLMethodIndexOf();
    method.getArguments().addAll(getArguments());
    return method;
  }
}

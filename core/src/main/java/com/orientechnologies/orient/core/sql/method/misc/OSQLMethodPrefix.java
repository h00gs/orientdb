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
public class OSQLMethodPrefix extends OAbstractSQLMethod {

  public static final String NAME = "prefix";

  public OSQLMethodPrefix() {
    super(NAME, 1);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    final List<OExpression> arguments = getMethodArguments();
    Object value = getSource().evaluate(context, candidate);

    final Object v = arguments.get(0).evaluate(context, candidate).toString();
    if (v != null) {
      value = value != null ? v + value.toString() : null;
    }
    return value;
  }
  
  @Override
  public OSQLMethodPrefix copy() {
    final OSQLMethodPrefix method = new OSQLMethodPrefix();
    method.getArguments().addAll(getArguments());
    return method;
  }
}

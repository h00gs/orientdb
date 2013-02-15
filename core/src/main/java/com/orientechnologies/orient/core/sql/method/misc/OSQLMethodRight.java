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
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OExpression;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodRight extends OSQLMethod {

  public static final String NAME = "right";

  public OSQLMethodRight() {
    super(NAME, 1);
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final List<OExpression> arguments = getMethodArguments();
    Object value = getSource().evaluate(context, candidate);
    final int offset = Integer.parseInt(arguments.get(0).evaluate(context, candidate).toString());
    value = value != null ? value.toString().substring(
            offset < value.toString().length() ? value.toString().length() - offset : 0) : null;
    return value;
  }
  
  @Override
  public OSQLMethodRight copy() {
    final OSQLMethodRight method = new OSQLMethodRight();
    method.getArguments().addAll(getArguments());
    return method;
  }
}

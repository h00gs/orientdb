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

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodAsBoolean extends OAbstractSQLMethod {

  public static final String NAME = "asboolean";

  public OSQLMethodAsBoolean() {
    super(NAME);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    Object value = getSource().evaluate(context, candidate);
    if (value != null) {
      if (value instanceof String) {
        value = Boolean.valueOf(((String) value).trim());
      } else if (value instanceof Number) {
        final int bValue = ((Number) value).intValue();
        if (bValue == 0) {
          value = Boolean.FALSE;
        } else if (bValue == 1) {
          value = Boolean.TRUE;
        } else {
          // IGNORE OTHER VALUES
          value = null;
        }
      }
    }
    return value;
  }
  
  @Override
  public OSQLMethodAsBoolean copy() {
    final OSQLMethodAsBoolean method = new OSQLMethodAsBoolean();
    method.getArguments().addAll(getArguments());
    return method;
  }
}

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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import java.text.ParseException;
import java.util.Date;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodAsDateTime extends OSQLMethod {

  public static final String NAME = "asdatetime";

  public OSQLMethodAsDateTime() {
    super(NAME);
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    Object value = getSource().evaluate(context, candidate);
    if (value != null) {
      if (value instanceof Number) {
        value = new Date(((Number) value).longValue());
      } else if (!(value instanceof Date)) {
        try {
          value = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateTimeFormatInstance()
                  .parse(value.toString());
        } catch (ParseException ex) {
          return null;
        }
      }
    }
    return value;
  }
  
  @Override
  public OSQLMethodAsDateTime copy() {
    final OSQLMethodAsDateTime method = new OSQLMethodAsDateTime();
    method.getArguments().addAll(getArguments());
    return method;
  }
}

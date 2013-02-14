/*
 * Copyright 2012 Orient Technologies.
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
package com.orientechnologies.orient.core.sql.operator;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Default operator factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultSQLOperatorFactory implements OSQLOperatorFactory{
  
  private static final Map<String, Class> OPERATORS = new HashMap<String, Class>();
  static {
    // MISC FUNCTIONS
    OPERATORS.put(OSQLOperatorContains.NAME.toUpperCase(Locale.ENGLISH), OSQLOperatorContains.class);
    OPERATORS.put(OSQLOperatorContainsAll.NAME.toUpperCase(Locale.ENGLISH), OSQLOperatorContainsAll.class);
    OPERATORS.put(OSQLOperatorContainsKey.NAME.toUpperCase(Locale.ENGLISH), OSQLOperatorContainsKey.class);
    OPERATORS.put(OSQLOperatorContainsValue.NAME.toUpperCase(Locale.ENGLISH), OSQLOperatorContainsValue.class);
    OPERATORS.put(OSQLOperatorInstanceof.NAME.toUpperCase(Locale.ENGLISH), OSQLOperatorContains.class);
  }

  @Override
  public Set<String> getOperatorNames() {
    return OPERATORS.keySet();
  }

  @Override
  public boolean hasOperator(String name) {
    name = name.toUpperCase(Locale.ENGLISH);
    return OPERATORS.containsKey(name);
  }

  @Override
  public OSQLOperator createOperator(String name) {
    name = name.toUpperCase(Locale.ENGLISH);
    final Object obj = OPERATORS.get(name);

    if (obj == null)
      throw new OCommandExecutionException("Unknowned operator name :" + name);

    if (obj instanceof OSQLOperator)
      return (OSQLOperator) obj;
    else {
      // it's a class
      final Class<?> clazz = (Class<?>) obj;
      try {
        return (OSQLOperator) clazz.newInstance();
      } catch (Exception e) {
        throw new OCommandExecutionException("Error in creation of operator " + name
            + "(). Probably there is not an empty constructor or the constructor generates errors", e);
      }
    }

  }  
      
}

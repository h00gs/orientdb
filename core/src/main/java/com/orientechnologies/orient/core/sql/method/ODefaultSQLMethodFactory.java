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

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.method.misc.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default methods factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultSQLMethodFactory implements OSQLMethodFactory{

    private final Map<String,Class> methods = new HashMap<String, Class>();

    public ODefaultSQLMethodFactory() {
        methods.put(OSQLMethodAppend.NAME, OSQLMethodAppend.class);
        methods.put(OSQLMethodAsBoolean.NAME, OSQLMethodAsBoolean.class);
        methods.put(OSQLMethodAsDate.NAME, OSQLMethodAsDate.class);
        methods.put(OSQLMethodAsDateTime.NAME, OSQLMethodAsDateTime.class);
        methods.put(OSQLMethodAsDecimal.NAME, OSQLMethodAsDecimal.class);
        methods.put(OSQLMethodAsFloat.NAME, OSQLMethodAsFloat.class);
        methods.put(OSQLMethodAsInteger.NAME, OSQLMethodAsInteger.class);
        methods.put(OSQLMethodAsLong.NAME, OSQLMethodAsLong.class);
        methods.put(OSQLMethodAsString.NAME, OSQLMethodAsString.class);
        methods.put(OSQLMethodCharAt.NAME, OSQLMethodCharAt.class);
        methods.put(OSQLMethodField.NAME, OSQLMethodField.class);
        methods.put(OSQLMethodFormat.NAME, OSQLMethodFormat.class);
        methods.put(OSQLMethodIndexOf.NAME, OSQLMethodIndexOf.class);
        methods.put(OSQLMethodKeys.NAME, OSQLMethodKeys.class);
        methods.put(OSQLMethodLeft.NAME, OSQLMethodLeft.class);
        methods.put(OSQLMethodLength.NAME, OSQLMethodLength.class);
        methods.put(OSQLMethodNormalize.NAME, OSQLMethodNormalize.class);
        methods.put(OSQLMethodPrefix.NAME, OSQLMethodPrefix.class);
        methods.put(OSQLMethodReplace.NAME, OSQLMethodReplace.class);
        methods.put(OSQLMethodRight.NAME, OSQLMethodRight.class);
        methods.put(OSQLMethodSize.NAME, OSQLMethodSize.class);
        methods.put(OSQLMethodSubString.NAME, OSQLMethodSubString.class);
        methods.put(OSQLMethodToJSON.NAME, OSQLMethodToJSON.class);
        methods.put(OSQLMethodToLowerCase.NAME, OSQLMethodToLowerCase.class);
        methods.put(OSQLMethodToUpperCase.NAME, OSQLMethodToUpperCase.class);
        methods.put(OSQLMethodTrim.NAME, OSQLMethodTrim.class);
        methods.put(OSQLMethodValues.NAME, OSQLMethodValues.class);
        methods.put(OSQLMethodContains.NAME, OSQLMethodContains.class);
        methods.put(OSQLMethodContainsAll.NAME, OSQLMethodContainsAll.class);
        methods.put(OSQLMethodContainsKey.NAME, OSQLMethodContainsKey.class);
        methods.put(OSQLMethodContainsValue.NAME, OSQLMethodContainsValue.class);
        methods.put(OSQLMethodContainsText.NAME, OSQLMethodContainsText.class);
    }
        
    @Override
    public boolean hasMethod(String iName) {
      iName = iName.toLowerCase();
      return methods.containsKey(iName);
    }

    @Override
    public Set<String> getMethodNames() {
        return methods.keySet();
    }

  @Override
  public OSQLMethod createMethod(String name) throws OCommandExecutionException {
    name = name.toLowerCase();
    final Object obj = methods.get(name);

    if (obj == null) {
      throw new OCommandExecutionException("Unknowned method name :" + name);
    }

    if (obj instanceof OSQLMethod) {
      return (OSQLMethod) obj;
    } else {
      // it's a class
      final Class<?> clazz = (Class<?>) obj;
      try {
        return (OSQLMethod) clazz.newInstance();
      } catch (Exception e) {
        throw new OCommandExecutionException("Error in creation of method " + name
                + "(). Probably there is not an empty constructor or the constructor generates errors", e);
      }
    }
  }

}

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
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAppend;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsBoolean;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsDate;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsDateTime;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsDecimal;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsFloat;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsInteger;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsLong;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsString;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodCharAt;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFormat;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodIndexOf;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodKeys;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodLeft;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodLength;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodNormalize;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodPrefix;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodReplace;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodRight;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodSize;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodSubString;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToJSON;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToLowerCase;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToUpperCase;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodTrim;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodValues;
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

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
package com.orientechnologies.orient.core.sql;

import java.util.Map;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;
import com.orientechnologies.orient.core.sql.operator.OSQLOperator;
import com.orientechnologies.orient.core.sql.operator.OSQLOperatorFactory;

/**
 * Dynamic sql elements factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODynamicSQLElementFactory implements OCommandExecutorSQLFactory, OSQLOperatorFactory, OSQLFunctionFactory, OSQLMethodFactory {

  // Used by SQLEngine to register on the fly new elements
  static final Map<String, Object>                                       FUNCTIONS = new ConcurrentHashMap<String, Object>();
  static final Map<String, Object>                                       OPERATORS = new ConcurrentHashMap<String, Object>();
  static final Map<String, Object>                                       METHODS = new ConcurrentHashMap<String, Object>();
  static final Map<String, Class<? extends OCommandExecutorSQLAbstract>> COMMANDS  = new ConcurrentHashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();
  
  @Override
  public boolean hasFunction(final String name) {
    return FUNCTIONS.containsKey(name.toUpperCase(Locale.ENGLISH));
  }

  @Override
  public boolean hasMethod(String name) {
    return METHODS.containsKey(name.toUpperCase(Locale.ENGLISH));
  }
  
  @Override
  public boolean hasOperator(String name) {
    return OPERATORS.containsKey(name.toUpperCase(Locale.ENGLISH));
  }
  
  @Override
  public Set<String> getFunctionNames() {
    return FUNCTIONS.keySet();
  }
  
  @Override
  public Set<String> getMethodNames() {
    return METHODS.keySet();
  }
  
  @Override
  public Set<String> getOperatorNames() {
    return OPERATORS.keySet();
  }
  
  @Override
  public OSQLFunction createFunction(final String name) throws OCommandExecutionException {
    final Object obj = FUNCTIONS.get(name.toUpperCase(Locale.ENGLISH));

    if (obj == null) {
      throw new OCommandExecutionException("Unknowned function name :" + name);
    }

    if (obj instanceof OSQLFunction) {
      return (OSQLFunction) obj;
    } else {
      // it's a class
      final Class<?> clazz = (Class<?>) obj;
      try {
        return (OSQLFunction) clazz.newInstance();
      } catch (Exception e) {
        throw new OCommandExecutionException("Error in creation of function " + name
            + "(). Probably there is not an empty constructor or the constructor generates errors", e);
      }
    }
  }


  @Override
  public OSQLOperator createOperator(String name) throws OCommandExecutionException {
    final Object obj = FUNCTIONS.get(name.toUpperCase(Locale.ENGLISH));

    if (obj == null) {
      throw new OCommandExecutionException("Unknowned operator name :" + name);
    }

    if (obj instanceof OSQLOperator) {
      return (OSQLOperator) obj;
    } else {
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



  @Override
  public OSQLMethod createMethod(String name) {
    final Object obj = FUNCTIONS.get(name.toUpperCase(Locale.ENGLISH));

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
  
  @Override
  public Set<String> getCommandNames() {
    return COMMANDS.keySet();
  }

  @Override
  public OCommandExecutorSQLAbstract createCommand(final String name) throws OCommandExecutionException {
    final Class<? extends OCommandExecutorSQLAbstract> clazz = COMMANDS.get(name);

    if (clazz == null)
      throw new OCommandExecutionException("Unknowned command name :" + name);

    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new OCommandExecutionException("Error in creation of command " + name
          + "(). Probably there is not an empty constructor or the constructor generates errors", e);
    }
  }

}

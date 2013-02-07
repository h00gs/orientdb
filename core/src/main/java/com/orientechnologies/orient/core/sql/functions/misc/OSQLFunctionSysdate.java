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
package com.orientechnologies.orient.core.sql.functions.misc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Returns the current date time.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @see OSQLFunctionDate
 * 
 */
public class OSQLFunctionSysdate extends OSQLFunctionAbstract {
  public static final String NAME = "sysdate";

  private SimpleDateFormat   format;

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public OSQLFunctionSysdate() {
    super(NAME, 0, 2);
  }

  public boolean aggregateResults(final Object[] configuredParameters) {
    return false;
  }

  @Override
  public String getSyntax() {
    return "Syntax error: sysdate([<format>] [,<timezone>])";
  }
  
  @Override
  public OSQLFunctionSysdate copy() {
    final OSQLFunctionSysdate fct = new OSQLFunctionSysdate();
    fct.getArguments().addAll(getArguments());
    return fct;
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    if (children.isEmpty()){
      return new Date();
    }

    final String param0 = (String)children.get(0).evaluate(context, candidate);
    if (format == null) {      
      format = new SimpleDateFormat(param0);
      if (children.size() == 2){
        final String param1 = (String)children.get(1).evaluate(context, candidate);
        format.setTimeZone(TimeZone.getTimeZone(param1));
      }
    }

    return format.format(new Date());
  }
  
}

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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Formats content.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionFormat extends OSQLFunctionAbstract {
	public static final String	NAME	= "format";

	public OSQLFunctionFormat() {
		super(NAME, 2, -1);
	}

  @Override
	public String getSyntax() {
		return "Syntax error: format(<format>, <arg1> [,<argN>]*)";
	}
  
  @Override
  public OSQLFunctionFormat copy() {
    final OSQLFunctionFormat fct = new OSQLFunctionFormat();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Object[] args = new Object[children.size() - 1];

		for (int i = 0; i < args.length; ++i){
			args[i] = children.get(i+1).evaluate(context, candidate);
    }
    final String src = (String)children.get(0).evaluate(context, candidate);
		return String.format(src, args);
  }
  
}

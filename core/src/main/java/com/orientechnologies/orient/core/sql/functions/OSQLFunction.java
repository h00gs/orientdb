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
package com.orientechnologies.orient.core.sql.functions;

import java.util.List;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;
import com.orientechnologies.orient.core.sql.model.OExpressionWithChildren;

/**
 * Interface that defines a SQL Function. Functions can be state-less if registered as instance, or state-full when registered as
 * class. State-less functions are reused across queries, so don't keep any run-time information inside of it. State-full functions,
 * instead, stores Implement it and register it with: <code>OSQLParser.getInstance().registerFunction()</code> to being used by the
 * SQL engine.
 * 
 * ??? could it be possible to have a small piece of code here showing where to register a function using services ???
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunction extends OExpressionWithChildren implements Comparable<OSQLFunction> {

  public OSQLFunction() {
    super(null);
  }
  
  /**
	 * Function name, the name is used by the sql parser to identify a call this function.
	 * 
	 * @return String , function name, never null or empty.
	 */
  public abstract String getName();
  
  public List<OExpression> getArguments(){
    return children;
  }
  
	/**
	 * Minimum number of parameter this function must have.
	 * 
	 * @return minimum number of parameters
	 */
	public abstract int getMinParams();

	/**
	 * Maximum number of parameter this function can handle.
	 * 
	 * @return maximum number of parameters ??? -1 , negative or Integer.MAX_VALUE for unlimited ???
	 */
	public abstract int getMaxParams();

	/**
	 * Returns a convinient SQL String representation of the function.
	 * <p>
	 * Example :
	 * 
	 * <pre>
	 *  myFunction( param1, param2, [optionalParam3])
	 * </pre>
	 * 
	 * This text will be used in exception messages.
	 * 
	 * @return String , never null.
	 */
	public abstract String getSyntax();

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  @Override
  public abstract OSQLFunction copy();
  
}

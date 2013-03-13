/*
 * Copyright 2013 Geomatys
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
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;

/**
 * Evaluation sub expression
 *
 * @author Johann Sorel (Geomatys)
 */
public class OSQLFunctionEval extends OSQLFunctionAbstract {

  public static final String NAME = "eval";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public OSQLFunctionEval() {
    super(NAME, 1, 1);
  }

  @Override
  public boolean isContextFree() {
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    return true;
  }
  
  @Override
  public String getSyntax() {
    return "Syntax error: eval(<stringexpression>)";
  }

  @Override
  public OSQLFunctionEval copy() {
    final OSQLFunctionEval fct = new OSQLFunctionEval();
    fct.setAlias(alias);
    fct.getArguments().addAll(getArguments());
    return fct;
  }

  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {    
    final String str = children.get(0).evaluate(context, candidate).toString();
    final OExpression exp = SQLGrammarUtils.parseExpression(str);
    return exp.evaluate(context, candidate);
  }
  
}

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
package com.orientechnologies.orient.core.sql.model;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;

/**
 * An Expression is an unresolved operation which result change based
 * on the context and the tested object.
 * Use the evaluate method to obtain it's value.
 * 
 * @author Johann Sorel (Geomatys)
 */
public interface OExpression {
  
  /**
   * Expression which always return TRUE.
   */
  public static final Include INCLUDE = new Include();
  /**
   * Expression which always return FALSE.
   */
  public static final Exclude EXCLUDE = new Exclude();
  
  /**
   * Evaluate the expression.
   * 
   * @param context
   * @param candidate
   * @return Object  can be null
   */
  Object evaluate(OCommandContext context, Object candidate);
  
  /**
   * Get the alias of this expression.
   * Used in projections.
   * @param alias 
   */
  String getAlias();
  
  /**
   * Set the alias of this expression.
   * Used in projections.
   * @param alias 
   */
  void setAlias(String alias);
  
  /**
   * Check if the expression evaluation is affected by document or context.
   * If an expression is static it can be evaluated only once.
   * @return true if expression is document or context sensitive
   */
  boolean isStatic();
  
  /**
   * Check if the expression evaluation is affected by the passed context.
   * @return true if expression is context sensitive
   */
  boolean isContextFree();
  
  /**
   * Check if the expression evaluation is affected by the passed document.
   * @return true if expression is document sensitive
   */
  boolean isDocumentFree();
  
  /**
   * Estimate index use possibilities.
   * 
   * @param clazz
   * @param sorts
   * @return OIndexResult
   */
  OSearchResult searchIndex(OSearchContext searchContext);
  
  /**
   * Visitor pattern.
   * @param visitor
   * @param data
   * @return 
   */
  Object accept(OExpressionVisitor visitor, Object data);
  
  /**
   * Duplicate this expression
   * @return copy of this expression
   */
  OExpression copy();
  
  public static final class Include extends OExpressionAbstract{

    private Include() {}
    
    @Override
    protected String thisToString() {
      return "INCLUDE";
    }

    @Override
    public Object evaluate(OCommandContext context, Object candidate) {
      return Boolean.TRUE;
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
    public OSearchResult searchIndex(OSearchContext searchContext) {
      return null;
    }

    @Override
    public Object accept(OExpressionVisitor visitor, Object data) {
        return visitor.visit(this, data);
    }

    @Override
    public OExpression copy() {
      //immutable
      return this;
    }
    
  }
  
  public static final class Exclude extends OExpressionAbstract{

    private Exclude() {}
    
    @Override
    protected String thisToString() {
      return "EXCLUDE";
    }

    @Override
    public Object evaluate(OCommandContext context, Object candidate) {
      return Boolean.FALSE;
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
    public OSearchResult searchIndex(OSearchContext searchContext) {
      return null;
    }

    @Override
    public Object accept(OExpressionVisitor visitor, Object data) {
        return visitor.visit(this, data);
    }

    @Override
    public OExpression copy() {
      //immutable
      return this;
    }
    
  }
  
}

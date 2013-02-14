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

import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public interface OExpressionVisitor {

  Object visit(OAnd candidate, Object data);
  
  Object visit(OCollection candidate, Object data);
  
  Object visit(OContextVariable candidate, Object data);
  
  Object visit(OEquals candidate, Object data);
  
  Object visit(OExpression candidate, Object data);
  
  Object visit(OIn candidate, Object data);
  
  Object visit(OInferior candidate, Object data);
  
  Object visit(OInferiorEquals candidate, Object data);
  
  Object visit(OIsNotNull candidate, Object data);
  
  Object visit(OIsNull candidate, Object data);
  
  Object visit(OLike candidate, Object data);
    
  Object visit(OLiteral candidate, Object data);
    
  Object visit(OMap candidate, Object data);
  
  Object visit(OName candidate, Object data);
  
  Object visit(ONot candidate, Object data);
  
  Object visit(ONotEquals candidate, Object data);
  
  Object visit(OOperatorDivide candidate, Object data);
  
  Object visit(OOperatorMinus candidate, Object data);
  
  Object visit(OOperatorModulo candidate, Object data);
  
  Object visit(OOperatorMultiply candidate, Object data);
  
  Object visit(OOperatorPlus candidate, Object data);
  
  Object visit(OOperatorPower candidate, Object data);
  
  Object visit(OOr candidate, Object data);
  
  Object visit(OPath candidate, Object data);
  
  Object visit(OSuperior candidate, Object data);
  
  Object visit(OSuperiorEquals candidate, Object data);
  
  Object visit(OUnset candidate, Object data);
  
  Object visit(OSQLFunction candidate, Object data);
  
  Object visit(OSQLMethod candidate, Object data);
          
  Object visit(OQueryOperator candidate, Object data);
  
  Object visitInclude(OExpression candidate, Object data);
  
  Object visitExclude(OExpression candidate, Object data);
  
}

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
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.model.OCollection;
import com.orientechnologies.orient.core.sql.model.OContextVariable;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;
import com.orientechnologies.orient.core.sql.model.OField;
import com.orientechnologies.orient.core.sql.model.OFunction;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OMap;
import com.orientechnologies.orient.core.sql.model.OMethod;
import com.orientechnologies.orient.core.sql.model.OOperator;
import com.orientechnologies.orient.core.sql.model.OUnset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class CopyVisitor implements OExpressionVisitor {

  @Override
  public Object visit(OLiteral candidate, Object data) {
    //literals are immutable
    return candidate;
  }
  
  @Override
  public Object visit(OCollection candidate, Object data) {
    final List<OExpression> args = candidate.getChildren();
    for(int i=0;i<args.size();i++){
      args.set(i, (OExpression)args.get(i).accept(this, data));
    }
    OCollection copy = new OCollection(args);
    return copy;
  }

  @Override
  public Object visit(OMap candidate, Object data) {
    final LinkedHashMap map = new LinkedHashMap();
    for(Map.Entry<OLiteral,OExpression> entry : candidate.getMap().entrySet()){
      map.put(entry.getKey().accept(this, data), entry.getValue().accept(this, data));
    }
    return new OMap(map);
  }

  @Override
  public Object visit(OField candidate, Object data) {
    //fields are immutable
    return candidate;
  }

  @Override
  public Object visit(OContextVariable candidate, Object data) {
    //context variable are immutable
    return candidate;
  }

  @Override
  public Object visit(OUnset candidate, Object data) {
    //unset are immutable
    return candidate;
  }
  
  @Override
  public Object visit(OFunction candidate, Object data) {
    final List<OExpression> args = candidate.getArguments();
    for(int i=0;i<args.size();i++){
      args.set(i, (OExpression)args.get(i).accept(this, data));
    }
    OFunction copy = new OFunction(candidate.getName(), candidate.getAlias(), args);
    return copy;
  }
  
  @Override
  public Object visit(OMethod candidate, Object data) {
    final List<OExpression> args = candidate.getMethodArguments();
    final OExpression source = (OExpression)candidate.getSource().accept(this, data);
    for(int i=0;i<args.size();i++){
      args.set(i, (OExpression)args.get(i).accept(this, data));
    }
    OMethod copy = new OMethod(candidate.getName(), candidate.getAlias(), source, args);
    return copy;
  }
  
  @Override
  public Object visit(OOperator candidate, Object data) {
    final List<OExpression> args = candidate.getArguments();
    for(int i=0;i<args.size();i++){
      args.set(i, (OExpression)args.get(i).accept(this, data));
    }
    OOperator copy = new OOperator(candidate.getName(), candidate.getAlias(), args);
    return copy;
  }

  @Override
  public Object visit(OExpression candidate, Object data) {
    throw new UnsupportedOperationException("Unknowned expression :"+candidate.getClass());
  }

  
  
}

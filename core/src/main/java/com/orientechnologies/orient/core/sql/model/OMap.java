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
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OMap extends OExpressionAbstract {

  private final LinkedHashMap<OLiteral,OExpression> map;

  public OMap(LinkedHashMap<OLiteral,OExpression> map) {
    this.map = map;
  }

  public LinkedHashMap<OLiteral, OExpression> getMap() {
    return map;
  }

  @Override
  protected String thisToString() {
    final StringBuilder sb = new StringBuilder("(Map) ");
    for(Entry<OLiteral,OExpression> entry : map.entrySet()){
      sb.append(entry.toString());
    }
    return sb.toString();
  }

  @Override
  public Object evaluate(OCommandContext context, Object candidate) {
    final LinkedHashMap value = new LinkedHashMap();
    for(Entry<OLiteral,OExpression> entry : map.entrySet()){
      value.put(entry.getKey().evaluate(context, candidate), 
                entry.getValue().evaluate(context, candidate));
    }
    return value;
  }

  @Override
  public boolean isContextFree() {
    for(Entry<OLiteral,OExpression> entry : map.entrySet()){
      if(!entry.getKey().isContextFree()){
        return false;
      }else if(!entry.getValue().isContextFree()){
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    for(Entry<OLiteral,OExpression> entry : map.entrySet()){
      if(!entry.getKey().isDocumentFree()){
        return false;
      }else if(!entry.getValue().isDocumentFree()){
        return false;
      }
    }
    return true;
  }

  @Override
  public OIndexResult searchIndex(OClass clazz, OSortBy[] sorts) {
    return null;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  
}

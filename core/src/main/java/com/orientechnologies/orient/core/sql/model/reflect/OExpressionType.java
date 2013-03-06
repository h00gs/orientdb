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
package com.orientechnologies.orient.core.sql.model.reflect;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.model.OExpressionAbstract;
import com.orientechnologies.orient.core.sql.model.OExpressionVisitor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Reference to a document type.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class OExpressionType extends OExpressionAbstract {

  public OExpressionType() {
    this(null);
  }
  
  public OExpressionType(String alias) {
    super(alias);
    if(alias == null){
        setAlias("type");
    }
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    if(candidate instanceof ORID){
      candidate = ((ORID)candidate).getRecord();
    }
    
    if(candidate instanceof ORecordInternal){
      return Orient.instance().getRecordFactoryManager()
            .getRecordTypeName(((ORecordInternal<?>) candidate).getRecordType());
    }else if(candidate instanceof Map){
        candidate = ((Map)candidate).values();
    }
    
    if(candidate instanceof Collection){
        //regroup each elements in a list
        final List res = new ArrayList();
        for(Object o : (Collection)candidate){
            res.add(evaluateNow(context, o));
        }
        if(res.size() == 1){
            //extract result
            return res.get(0);
        }
        return res;
    }
    return null;
  }

  @Override
  public boolean isContextFree() {
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    return false;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  protected String thisToString() {
    return "(@Type) ";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return true;
  }
  
  @Override
  public OExpressionType copy() {
    return new OExpressionType(alias);
  }
  
}

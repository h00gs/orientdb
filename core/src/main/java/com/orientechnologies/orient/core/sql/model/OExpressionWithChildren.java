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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class OExpressionWithChildren extends OExpressionAbstract {
  
  protected final List<OExpression> children;

  public OExpressionWithChildren(List<OExpression> arguments){
    this(null,arguments);
  }
  
  public OExpressionWithChildren(String alias,OExpression ... children){
    this(null,Arrays.asList(children));
  }
  
  public OExpressionWithChildren(String alias, List<OExpression> arguments){
    super(alias);
    if(arguments == null){
      this.children = Collections.EMPTY_LIST;
    }else{
      this.children = Collections.unmodifiableList(arguments);
    }
  }

  public List<OExpression> getChildren() {
    return children;
  }
  
  @Override
  public boolean isContextFree() {
    for(OExpression exp : children){
      if(!exp.isContextFree()){
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDocumentFree() {
    for(OExpression exp : children){
      if(!exp.isDocumentFree()){
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 11 * hash + (this.children != null ? this.children.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof OExpressionWithChildren)) {
      return false;
    }
    final OExpressionWithChildren other = (OExpressionWithChildren) obj;
    if (this.children != other.children && (this.children == null || !this.children.equals(other.children))) {
      return false;
    }
    return true;
  }
  
  
}

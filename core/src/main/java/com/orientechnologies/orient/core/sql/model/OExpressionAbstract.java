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

import java.util.List;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class OExpressionAbstract implements OExpression{

  private final String alias;

  public OExpressionAbstract() {
    this(null);
  }

  public OExpressionAbstract(String alias) {
    this.alias = alias;
  }

  @Override
  public String getAlias() {
    return alias;
  }
  
  @Override
  public final boolean isStatic() {
    return isContextFree() && isDocumentFree();
  }

  protected abstract String thisToString();
  
  @Override
  public final String toString() {
    return toString(this);
  }
  
  private static String toString(OExpression candidate){
    if(candidate instanceof OExpressionAbstract){
      final OExpressionAbstract exp = (OExpressionAbstract) candidate;
      final StringBuilder sb = new StringBuilder();
      sb.append(exp.thisToString());
      
      if(candidate instanceof OExpressionWithChildren){
        final OExpressionWithChildren fct = (OExpressionWithChildren) candidate;
        final List<OExpression> children = fct.getChildren();
        //print childrens
        final int nbChild = children.size();
        if(nbChild>0){
            sb.append('\n');
            for(int i=0;i<nbChild;i++){
                if(i==nbChild-1){
                    sb.append("\u2514\u2500 ");
                }else{
                    sb.append("\u251C\u2500 ");
                }

                String sc = toString(children.get(i));
                String[] parts = sc.split("\n");
                sb.append(parts[0]).append('\n');
                for(int k=1;k<parts.length;k++){
                    if(i==nbChild-1){
                        sb.append(' ');
                    }else{
                        sb.append('\u2502');
                    }
                    sb.append("    ");
                    sb.append(parts[k]);
                    sb.append('\n');
                }
            }
        }
      }
      return sb.toString();
    }else{
      return candidate.toString();
    }
  }
  
}

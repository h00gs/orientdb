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

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import java.text.ParseException;

import java.util.Collection;
import java.util.Date;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class OInferior extends OExpressionWithChildren{
  
  public OInferior(OExpression left, OExpression right) {
    this(null,left,right);
  }

  public OInferior(String alias, OExpression left, OExpression right) {
    super(alias,left,right);
  }
  
  public OExpression getLeft(){
    return children.get(0);
  }
  
  public OExpression getRight(){
    return children.get(1);
  }
  
  @Override
  protected String thisToString() {
    return "(<)";
  }

  @Override
  protected void analyzeSearchIndex(OSearchContext searchContext, OSearchResult result) {
    final String className = searchContext.getSource().getTargetClasse();
    if(className == null){
      //no optimisation
      return;
    }
    
    //test is inferior match pattern : field < value
    final boolean under;
    OName fieldName;
    OLiteral literal;
    if(getLeft() instanceof OName && getRight() instanceof OLiteral){
      fieldName = (OName) getLeft();
      literal = (OLiteral) getRight();
      under = true;
    }else if(getLeft() instanceof OLiteral && getRight() instanceof OName){
      fieldName = (OName) getRight();
      literal = (OLiteral) getLeft();
      under = false;
    }else{
      //no optimisation
      return;
    }
    
    //search for an index
    final OClass clazz = getDatabase().getMetadata().getSchema().getClass(className);
    final Set<OIndex<?>> indexes = clazz.getClassInvolvedIndexes(fieldName.getName());
    if(indexes == null || indexes.isEmpty()){
      //no index usable
      return;
    }
    
    for(OIndex index : indexes){
      if(index.getKeyTypes().length != 1){
        continue;
      }
      
      final Object key = literal.evaluate(null, null);
      
      //found a usable index
      final Collection<OIdentifiable> ids;
      if(under){
          ids = index.getValuesMinor(key, false);
      }else{
          ids = index.getValuesMajor(key, false);
      }
      searchResult.setState(OSearchResult.STATE.FILTER);
      searchResult.setIncluded(ids);
      updateStatistic(index);
      return;
    }
    
  }
  
  @Override
  protected Object evaluateNow(OCommandContext context, Object candidate) {
    final Integer v = compare(getLeft(),getRight(),context,candidate);
    return (v == null) ? false : (v < 0) ;
  }

  @Override
  public Object accept(OExpressionVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return super.equals(obj);
  }
  
  /**
   * Generic compare method
   *
   * @param object
   * @return Integer or null
   */
  static Integer compare(OExpression left, OExpression right, OCommandContext context, Object candidate) {
    final Object objleft = left.evaluate(context, candidate);
    if (!(objleft instanceof Comparable)) {
      return null;
    }
    final Object objright = right.evaluate(context, candidate);
    return compare(objleft, objright);
  }
  
  static Integer compare(Object objleft, Object objright) {

    if(objleft == null && objright == null){
        return 0;
    }else if(objleft == null || objright == null){
        //can't compare
        return null;
    }

    if(objleft instanceof Date){
      objleft = ((Date)objleft).getTime();
      //if right is a String, try to convert it to a Date
      if(objright instanceof String){
        try{
            objright = SQLGrammarUtils.toDate((String)objright);
        }catch(ParseException ex){ 
            //we tryed 
        }
      }
    }
    if(objright instanceof Date){
      objright = ((Date)objright).getTime();
      //if left is a String, try to convert it to a Date
      if(objleft instanceof String){
        try{
            SQLGrammarUtils.toDate((String)objleft).getTime();
        }catch(ParseException ex){ 
            //we tryed 
        }
      }
    }

    if(objleft instanceof Number && objright instanceof Number){
      final Double dl = (Double)((Number)objleft).doubleValue();
      final Double dr = (Double)((Number)objright).doubleValue();
      return dl.compareTo(dr);
    }

    if(objleft instanceof OCompositeKey){
        //try to convert right to Compositekey
        final OCompositeKey lkey = (OCompositeKey) objleft;
        OCompositeKey rkey = null;
        if(objright instanceof Collection){
            rkey = new OCompositeKey( ((Collection)objright).toArray() );
        }else{
            rkey = new OCompositeKey( objright );
        }
        return lkey.compareTo(rkey);
    }else if(objright instanceof OCompositeKey){
        //try to convert left to Compositekey
        final OCompositeKey rkey = (OCompositeKey) objright;
        OCompositeKey lkey = null;
        if(objleft instanceof Collection){
            lkey = new OCompositeKey( ((Collection)objleft).toArray() );
        }else{
            lkey = new OCompositeKey( objleft );
        }
        return lkey.compareTo(rkey);
    }

    if(objleft instanceof Comparable && objright instanceof Comparable){
        try{
            return ((Comparable) objleft).compareTo(objright);
        }catch(Exception ex){
            // we tried
        }
    }
    
    //check if we are dealing with orid
    try{
        final ORID ol = toORID(objleft);
        final ORID or = toORID(objright);
        return ol.compareTo(or);
    }catch(Exception ex){
        //we tryed
    }

    return null;
  }

  private static ORID toORID(Object candidate) throws Exception{
      if(candidate instanceof ORID){
          return (ORID) candidate;
      }else if (candidate instanceof String){
          return new ORecordId((String)candidate);
      }else if (candidate instanceof ODocument){
          return ((ODocument)candidate).getIdentity();
      }
      throw new Exception("Not an ORID");
  }
  
  @Override
  public OInferior copy() {
    return new OInferior(alias, getLeft(),getRight());
  }
  
}

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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OFunction;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OMethod;
import com.orientechnologies.orient.core.sql.command.OCommandCustom;
import com.orientechnologies.orient.core.sql.model.OAnd;
import com.orientechnologies.orient.core.sql.model.OCollection;
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OName;
import com.orientechnologies.orient.core.sql.model.OIsNotNull;
import com.orientechnologies.orient.core.sql.model.OIsNull;
import com.orientechnologies.orient.core.sql.model.OMap;
import com.orientechnologies.orient.core.sql.model.ONot;
import com.orientechnologies.orient.core.sql.model.OOr;
import com.orientechnologies.orient.core.sql.model.OUnset;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;

import static com.orientechnologies.orient.core.sql.parser.OSQLParser.*;
import java.util.LinkedHashMap;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public final class SQLGrammarUtils {

  private SQLGrammarUtils() {
  }
  
  
  public static OCommandExecutor visit(OSQLParser.CommandContext candidate) {
    
    final OCommandExecutor command;
    final Object commandTree = candidate.getChild(0);
    if(commandTree instanceof OSQLParser.CommandUnknownedContext){
      command = visit((OSQLParser.CommandUnknownedContext)commandTree);
    }else{
      throw new OException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    return command;
  }
    
  private static OCommandCustom visit(OSQLParser.CommandUnknownedContext candidate){
    //variables
    final List<Object> elements = new ArrayList<Object>();
    
    final int nb = candidate.getChildCount();
    for(int i=0;i<nb;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add(SQLGrammarUtils.visit(child));
    }
    
    return new OCommandCustom(elements);
  }
  
  public static Object visit(ParseTree candidate){
    if(candidate instanceof ExpressionContext){
      return visit((ExpressionContext)candidate);
    }else if(candidate instanceof WordContext){
      return visit((WordContext)candidate);
    }else if(candidate instanceof LiteralContext){
      return visit((LiteralContext)candidate);
    }else if(candidate instanceof FunctionCallContext){
      return visit((FunctionCallContext)candidate);
    }else if(candidate instanceof MethodCallContext){
      return visit((MethodCallContext)candidate);
    }else if(candidate instanceof IdentifierContext){
      return visit((IdentifierContext)candidate);
    }else if(candidate instanceof MapContext){
      return visit((MapContext)candidate);
    }else if(candidate instanceof CollectionContext){
      return visit((CollectionContext)candidate);
    }else if(candidate instanceof UnsetContext){
      return visit((UnsetContext)candidate);
    }else if(candidate instanceof FilterContext){
      return visit((FilterContext)candidate);
    }else{
      throw new OException("Unexpected parse tree element :"+candidate.getClass()+" "+candidate);
    }
  }
  
  public static OExpression visit(ExpressionContext candidate){
    final int nbChild = candidate.getChildCount();
    final List<OExpression> elements = new ArrayList<OExpression>(nbChild);
    for(int i=0;i<nbChild;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add((OExpression)visit(child));
    }
    
    if(nbChild == 1){
      //can be a word, literal, functionCall
      return elements.get(0);
    }else if(nbChild == 2){
      //can be a method call
      final OExpression source = (OExpression) elements.get(0);
      final OMethod method = (OMethod) elements.get(1);
      final OMethod res = new OMethod(method.getName(), source, method.getMethodArguments());
      return res;
    }else if(nbChild == 3){
      //can be '(' exp ')'
      return elements.get(1);
    }else{
      throw new OException("Unexpected number of arguments");
    }
    
  }
  
  public static OName visit(WordContext candidate){
    return new OName(candidate.WORD().getText());
  }
  
  public static OUnset visit(UnsetContext candidate){
    return new OUnset();
  }
  
  public static OLiteral visit(IdentifierContext candidate){
    final ORecordId oid = new ORecordId(candidate.getText());
    return new OLiteral(oid);
  } 
  
  public static OCollection visit(CollectionContext candidate) {
    final List col = new ArrayList();
    final List<ExpressionContext> values = candidate.expression();
    for (int i = 0, n = values.size(); i < n; i++) {
      col.add(visit(values.get(i)));
    }
    return new OCollection(col);
  }

  public static OMap visit(MapContext candidate) {
    final LinkedHashMap map = new LinkedHashMap();
    final List<LiteralContext> keys = candidate.literal();
    final List<ExpressionContext> values = candidate.expression();
    for (int i = 0, n = keys.size(); i < n; i++) {
      map.put(visit(keys.get(i)), visit(values.get(i)));
    }
    return new OMap(map);
  }
  
  public static OLiteral visit(LiteralContext candidate){
    if(candidate.TEXT() != null){
      String txt =candidate.TEXT().getText();
      txt = txt.substring(1,txt.length()-1);
      return new OLiteral(txt);
      
    }else if(candidate.number()!= null){
      final NumberContext n = candidate.number();
      if(n.INT() != null){
        return new OLiteral(Integer.valueOf(n.getText()));
      }else{
        return new OLiteral(Double.valueOf(n.getText()));
      }
      
    }else if(candidate.NULL()!= null){
      return new OLiteral(null);
      
    }else{
      throw new OException("Should not happen");
    }
  }
  
  public static OFunction visit(FunctionCallContext candidate){
    final String name = ((WordContext)candidate.getChild(0)).getText();
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(1)) );
    return new OFunction(name, args);
  }
  
  public static OMethod visit(MethodCallContext candidate){
    final String name = ((WordContext)candidate.getChild(1)).getText();
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(2)) );
    return new OMethod(name, null, args);
  }
    
  public static OExpression visit(OSQLParser.ProjectionContext candidate) {
    
    OExpression exp;
    if(candidate.word() != null){
      exp = new OName(candidate.word().getText());
    }else{
      throw new OException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    return exp;
  }
  
  public static OExpression visit(OSQLParser.FilterContext candidate){
    final int nbChild = candidate.getChildCount();
    if(nbChild == 1){
      //can be a word, literal, functionCall
      return (OExpression) visit(candidate.getChild(0));
    }else if(nbChild == 2){
      //can be :
      //filter filterAnd
      //filter filterOr
      //NOT filter
      if(candidate.filterAnd() != null){
        return new OAnd(
                (OExpression)visit(candidate.getChild(0)), 
                (OExpression)visit(candidate.filterAnd().filter()));
      }else if(candidate.filterOr() != null){
        return new OOr(
                (OExpression)visit(candidate.getChild(0)), 
                (OExpression)visit(candidate.filterOr().filter()));
      }else if(candidate.NOT() != null){
        return new ONot(
                (OExpression)visit(candidate.getChild(1)));
      }else{
        throw new OException("Unexpected arguments");
      }
    }else if(nbChild == 3){
      //can be :
      // '(' filter ')'
      //filter EQUALS filter
      //filter IS NULL
      if(candidate.EQUALS() != null){
        return new OEquals(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.IS()!= null){
        return new OIsNull(
              (OExpression) visit(candidate.getChild(0)));
      }else{
        return (OExpression) visit(candidate.getChild(1));
      }
      
    }else if(nbChild == 4){
      //can be :
      //filter IS NOT NULL
      return new OIsNotNull(
              (OExpression) visit(candidate.getChild(0)));
    }else{
      throw new OException("Unexpected number of arguments");
    }
  }
  
  public static List<OExpression> visit(ArgumentsContext candidate){
    final int nbChild = candidate.getChildCount();
    final List<OExpression> elements = new ArrayList<OExpression>(nbChild);
    for(int i=1;i<nbChild-1;i+=2){
      final ParseTree child = candidate.getChild(i);
      elements.add((OExpression)visit(child));
    }
    return elements;
  }
  
}

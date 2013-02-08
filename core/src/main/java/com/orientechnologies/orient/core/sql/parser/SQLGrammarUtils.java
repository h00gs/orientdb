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

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.command.OCommandCustom;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;
import com.orientechnologies.orient.core.sql.model.OAnd;
import com.orientechnologies.orient.core.sql.model.OCollection;
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OIn;
import com.orientechnologies.orient.core.sql.model.OInferior;
import com.orientechnologies.orient.core.sql.model.OInferiorEquals;
import com.orientechnologies.orient.core.sql.model.OName;
import com.orientechnologies.orient.core.sql.model.OIsNotNull;
import com.orientechnologies.orient.core.sql.model.OIsNull;
import com.orientechnologies.orient.core.sql.model.OMap;
import com.orientechnologies.orient.core.sql.model.ONot;
import com.orientechnologies.orient.core.sql.model.ONotEquals;
import com.orientechnologies.orient.core.sql.model.OOr;
import com.orientechnologies.orient.core.sql.model.OSuperior;
import com.orientechnologies.orient.core.sql.model.OSuperiorEquals;
import com.orientechnologies.orient.core.sql.model.OUnset;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;

import static com.orientechnologies.orient.core.sql.parser.OSQLParser.*;
import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.model.OLike;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public final class SQLGrammarUtils {

  private static ClassLoader CLASSLOADER = SQLGrammarUtils.class.getClassLoader();
  
  private SQLGrammarUtils() {
  }
  
  
  public static OCommandExecutor visit(OSQLParser.CommandContext candidate) throws SyntaxException {
    
    final OCommandExecutor command;
    final Object commandTree = candidate.getChild(0);
    if(commandTree instanceof OSQLParser.CommandUnknownedContext){
      command = visit((OSQLParser.CommandUnknownedContext)commandTree);
    }else{
      throw new SyntaxException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    return command;
  }
    
  private static OCommandCustom visit(OSQLParser.CommandUnknownedContext candidate) throws SyntaxException {
    //variables
    final List<Object> elements = new ArrayList<Object>();
    
    final int nb = candidate.getChildCount();
    for(int i=0;i<nb;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add(SQLGrammarUtils.visit(child));
    }
    
    return new OCommandCustom(elements);
  }
  
  public static Object visit(ParseTree candidate) throws SyntaxException {
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
      throw new SyntaxException("Unexpected parse tree element :"+candidate.getClass()+" "+candidate);
    }
  }
  
  public static OExpression visit(ExpressionContext candidate) throws SyntaxException {
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
      final OSQLMethod method = (OSQLMethod) elements.get(1);
      method.getArguments().add(0, source); //add the source as first argument.
      return method;
    }else if(nbChild == 3){
      //can be '(' exp ')'
      return elements.get(1);
    }else{
      throw new SyntaxException("Unexpected number of arguments");
    }
    
  }
  
  public static OName visit(WordContext candidate) throws SyntaxException {
    return new OName(candidate.WORD().getText());
  }
  
  public static OUnset visit(UnsetContext candidate) throws SyntaxException {
    return new OUnset();
  }
  
  public static OLiteral visit(IdentifierContext candidate) throws SyntaxException {
    final ORecordId oid = new ORecordId(candidate.getText());
    return new OLiteral(oid);
  } 
  
  public static OCollection visit(CollectionContext candidate) throws SyntaxException {
    final List col = new ArrayList();
    final List<ExpressionContext> values = candidate.expression();
    for (int i = 0, n = values.size(); i < n; i++) {
      col.add(visit(values.get(i)));
    }
    return new OCollection(col);
  }

  public static OMap visit(MapContext candidate) throws SyntaxException {
    final LinkedHashMap map = new LinkedHashMap();
    final List<LiteralContext> keys = candidate.literal();
    final List<ExpressionContext> values = candidate.expression();
    for (int i = 0, n = keys.size(); i < n; i++) {
      map.put(visit(keys.get(i)), visit(values.get(i)));
    }
    return new OMap(map);
  }
  
  public static OLiteral visit(LiteralContext candidate) throws SyntaxException {
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
      throw new SyntaxException("Should not happen");
    }
  }
  
  public static OSQLFunction visit(FunctionCallContext candidate) throws SyntaxException {
    final String name = ((WordContext)candidate.getChild(0)).getText();
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(1)) );
    final OSQLFunction fct = createFunction(name);
    fct.getArguments().addAll(args);
    return fct;
  }
  
  public static OSQLMethod visit(MethodCallContext candidate) throws SyntaxException {
    final String name = ((WordContext)candidate.getChild(1)).getText();
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(2)) );
    final OSQLMethod method = createMethod(name);
    method.getArguments().addAll(args);
    return method;
  }
    
  public static OExpression visit(OSQLParser.ProjectionContext candidate) throws SyntaxException {
    
    OExpression exp;
    if(candidate.filter() != null){
      exp = visit(candidate.filter());
    }else if(candidate.expression()!= null){
      exp = visit(candidate.expression());
    }else{
      throw new SyntaxException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    if(candidate.alias() != null){
      exp.setAlias(candidate.alias().word().getText());
    }
    
    return exp;
  }
  
  public static OExpression visit(OSQLParser.FilterContext candidate) throws SyntaxException {
    final int nbChild = candidate.getChildCount();
    if(nbChild == 1){
      //can be a word, literal, functionCall
      return (OExpression) visit(candidate.getChild(0));
    }else if(nbChild == 2){
      //can be :
      //filter filterAnd
      //filter filterOr
      //filter filterIn
      //NOT filter
      if(candidate.filterAnd() != null){
        return new OAnd(
                (OExpression)visit(candidate.getChild(0)), 
                (OExpression)visit(candidate.filterAnd().filter()));
      }else if(candidate.filterOr() != null){
        return new OOr(
                (OExpression)visit(candidate.getChild(0)), 
                (OExpression)visit(candidate.filterOr().filter()));
      }else if(candidate.filterIn() != null){
        final OExpression left = (OExpression)visit(candidate.getChild(0));
        final OExpression right;
        if(candidate.filterIn().literal() != null){
          right = (OExpression)visit(candidate.filterIn().literal());
        }else if(candidate.filterIn().collection() != null){
          right = (OExpression)visit(candidate.filterIn().collection());
        }else{
          throw new SyntaxException("Unexpected arguments");
        }
        return new OIn(left,right);
      }else if(candidate.NOT() != null){
        return new ONot(
                (OExpression)visit(candidate.getChild(1)));
      }else{
        throw new SyntaxException("Unexpected arguments");
      }
    }else if(nbChild == 3){
      //can be :
      // '(' filter ')'
      //filter COMPARE_X filter
      //filter IS NULL
      //filter LIKE filter
      if(candidate.COMPARE_EQL()!= null){
        return new OEquals(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.COMPARE_DIF()!= null){
        return new ONotEquals(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.COMPARE_INF()!= null){
        return new OInferior(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.COMPARE_INF_EQL()!= null){
        return new OInferiorEquals(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.COMPARE_SUP()!= null){
        return new OSuperior(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.COMPARE_SUP_EQL()!= null){
        return new OSuperiorEquals(
                (OExpression) visit(candidate.getChild(0)),
                (OExpression) visit(candidate.getChild(2)));
      }else if(candidate.LIKE()!= null){
        return new OLike(
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
      throw new SyntaxException("Unexpected number of arguments");
    }
  }
  
  public static List<OExpression> visit(ArgumentsContext candidate) throws SyntaxException {
    final int nbChild = candidate.getChildCount();
    final List<OExpression> elements = new ArrayList<OExpression>(nbChild);
    for(int i=1;i<nbChild-1;i+=2){
      final ParseTree child = candidate.getChild(i);
      elements.add((OExpression)visit(child));
    }
    return elements;
  }
  
  public static OSQLMethod createMethod(String name) throws SyntaxException{
    name = name.toLowerCase();
    final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, CLASSLOADER);
    while (ite.hasNext()) {
      final OSQLMethodFactory factory = ite.next();
      if (factory.hasMethod(name)) {
        return factory.createMethod(name);
      }
    }
    throw new SyntaxException("No method for name : "+name);
  }
  
  public static OSQLFunction createFunction(String name) throws SyntaxException{
    name = name.toLowerCase();
    final Iterator<OSQLFunctionFactory> ite = lookupProviderWithOrientClassLoader(OSQLFunctionFactory.class, CLASSLOADER);
    while (ite.hasNext()) {
      final OSQLFunctionFactory factory = ite.next();
      if (factory.hasFunction(name)) {
        return factory.createFunction(name);
      }
    }
    throw new SyntaxException("No function for name : "+name);
  }
  
}

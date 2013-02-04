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
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OFunction;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OMethod;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class AntlrToOrientVisitor {
    
  public List<Object> visit(OSQLParser.SentenceContext candidate) {
    final List<Object> elements = new ArrayList<Object>();
    
    final int nb = candidate.getChildCount();
    for(int i=0;i<nb;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add(visit(child));
    }
    
    
    return elements;
  }
  
  private Object visit(ParseTree candidate){
    if(candidate instanceof OSQLParser.ExpressionContext){
      return visit((OSQLParser.ExpressionContext)candidate);
    }else if(candidate instanceof OSQLParser.WordContext){
      return visit((OSQLParser.WordContext)candidate);
    }else if(candidate instanceof OSQLParser.LiteralContext){
      return visit((OSQLParser.LiteralContext)candidate);
    }else if(candidate instanceof OSQLParser.FunctionCallContext){
      return visit((OSQLParser.FunctionCallContext)candidate);
    }else if(candidate instanceof OSQLParser.MethodCallContext){
      return visit((OSQLParser.MethodCallContext)candidate);
    }else{
      throw new OException("Unexpected parse tree element :"+candidate.getClass()+" "+candidate);
    }
  }
  
  private Object visit(OSQLParser.ExpressionContext candidate){
    final int nbChild = candidate.getChildCount();
    final List<Object> elements = new ArrayList<Object>(nbChild);
    for(int i=0;i<nbChild;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add(visit(child));
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
  
  private String visit(OSQLParser.WordContext candidate){
    return candidate.WORD().getText();
  }
  
  private OLiteral visit(OSQLParser.LiteralContext candidate){
    if(candidate.TEXT1() != null){
      String txt =candidate.TEXT1().getText();
      txt = txt.substring(1,txt.length()-1);
      return new OLiteral(txt);
    }else if(candidate.TEXT2() != null){
      String txt =candidate.TEXT2().getText();
      txt = txt.substring(1,txt.length()-1);
      return new OLiteral(txt);
    }else if(candidate.literal_number() != null){
      return new OLiteral(Double.valueOf(candidate.literal_number().getText()));
    }else{
      throw new OException("Should not happen");
    }
  }
  
  private OFunction visit(OSQLParser.FunctionCallContext candidate){
    final String name = visit( ((OSQLParser.WordContext)candidate.getChild(0)) );
    final List<OExpression> args = visit( ((OSQLParser.ArgumentsContext)candidate.getChild(1)) );
    return new OFunction(name, args);
  }
  
  private OMethod visit(OSQLParser.MethodCallContext candidate){
    final String name = visit( ((OSQLParser.WordContext)candidate.getChild(1)) );
    final List<OExpression> args = visit( ((OSQLParser.ArgumentsContext)candidate.getChild(2)) );
    return new OMethod(name, null, args);
  }
    
  private List<OExpression> visit(OSQLParser.ArgumentsContext candidate){
    final int nbChild = candidate.getChildCount();
    final List<OExpression> elements = new ArrayList<OExpression>(nbChild);
    for(int i=1;i<nbChild-1;i+=2){
      final ParseTree child = candidate.getChild(i);
      elements.add((OExpression)visit(child));
    }
    return elements;
  }
  
}

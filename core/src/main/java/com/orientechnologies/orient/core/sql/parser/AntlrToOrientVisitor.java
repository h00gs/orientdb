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
import com.orientechnologies.orient.core.sql.command.OCommand;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OFunction;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OMethod;
import com.orientechnologies.orient.core.sql.command.OCommandCustom;
import com.orientechnologies.orient.core.sql.command.OCommandInsert;
import com.orientechnologies.orient.core.sql.model.OUnset;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;

import static com.orientechnologies.orient.core.sql.parser.OSQLParser.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class AntlrToOrientVisitor {
    
  public OCommandExecutor visit(CommandContext candidate) {
    
    final OCommandExecutor command;
    final Object commandTree = candidate.getChild(0);
    if(commandTree instanceof CommandUnknownedContext){
      command = visit((CommandUnknownedContext)commandTree);
    }else if(commandTree instanceof CommandInsertIntoByValuesContext){
      command = visit((CommandInsertIntoByValuesContext)commandTree);
    }else if(commandTree instanceof CommandInsertIntoBySetContext){
      command = visit((CommandInsertIntoBySetContext)commandTree);
    }else{
      throw new OException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    return command;
  }
    
  private OCommandCustom visit(CommandUnknownedContext candidate){
    //variables
    final List<Object> elements = new ArrayList<Object>();
    
    final int nb = candidate.getChildCount();
    for(int i=0;i<nb;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add(visit(child));
    }
    
    return new OCommandCustom(elements);
  }
  
  private OCommandInsert visit(CommandInsertIntoByValuesContext candidate){    
    //variables
    final String target;
    final List<String> fields = new ArrayList<String>();
    final List<Object[]> entries = new ArrayList<Object[]>();
    
    //parsing
    target = visit(candidate.word());
    for(WordContext wc : candidate.commandInsertIntoFields().word()){
      fields.add(visit(wc));
    }
    for(CommandInsertIntoEntryContext entry : candidate.commandInsertIntoEntry()){
      final List<ExpressionContext> exps = entry.expression();
      final Object[] values = new Object[exps.size()];
      for(int i=0;i<values.length;i++){
        values[i] = visit(exps.get(i));
      }
      entries.add(values);
    }
    
    return new OCommandInsert(target, fields, entries);
  }
  
  private OCommandInsert visit(CommandInsertIntoBySetContext candidate){    
    //variables
    final String target;
    final List<String> fields = new ArrayList<String>();
    final List<Object> values = new ArrayList<Object>();
    
    //parsing
    target = visit(candidate.word());
    for(CommandInsertIntoSetContext entry : candidate.commandInsertIntoSet()){
      final String att = visit(entry.word());
      fields.add(att);
      final ExpressionContext exp = entry.expression();
      values.add(visit(exp));
    }
    
    final List<Object[]> entries = new ArrayList<Object[]>();
    entries.add(values.toArray());
    return new OCommandInsert(target, fields, entries);
  }
  
  private Object visit(ParseTree candidate){
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
    }else if(candidate instanceof UnsetContext){
      return visit((UnsetContext)candidate);
    }else{
      throw new OException("Unexpected parse tree element :"+candidate.getClass()+" "+candidate);
    }
  }
  
  private Object visit(ExpressionContext candidate){
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
  
  private String visit(WordContext candidate){
    return candidate.WORD().getText();
  }
  
  private OUnset visit(UnsetContext candidate){
    return new OUnset();
  }
  
  private OLiteral visit(LiteralContext candidate){
    if(candidate.TEXT() != null){
      String txt =candidate.TEXT().getText();
      txt = txt.substring(1,txt.length()-1);
      return new OLiteral(txt);
      
    }else if(candidate.literal_number() != null){
      final Literal_numberContext n = candidate.literal_number();
      if(n.INT() != null){
        return new OLiteral(Integer.valueOf(n.getText()));
      }else{
        return new OLiteral(Double.valueOf(n.getText()));
      }
      
    }else if(candidate.NULL()!= null){
      return new OLiteral(null);
      
    }else if(candidate.identifier()!= null){
      final ORecordId oid = new ORecordId(candidate.identifier().getText());
      return new OLiteral(oid);
      
    }else if(candidate.literal_map() != null){
      final Literal_mapContext m = candidate.literal_map();
      final Map map = new LinkedHashMap();
      final List<TerminalNode> keys = m.TEXT();
      final List<ExpressionContext> values = m.expression();
      for(int i=0,n=keys.size();i<n;i++){
        map.put(keys.get(i).getText(), OSQL.evaluate(visit(values.get(i))));
      }
      return new OLiteral(map);
      
    }else if(candidate.literal_collection()!= null){
      final Literal_collectionContext m = candidate.literal_collection();
      final List col = new ArrayList();
      final List<ExpressionContext> values = m.expression();
      for(int i=0,n=values.size();i<n;i++){
        col.add(OSQL.evaluate(visit(values.get(i))));
      }
      return new OLiteral(col);
      
    }else{
      throw new OException("Should not happen");
    }
  }
  
  private OFunction visit(FunctionCallContext candidate){
    final String name = visit( ((WordContext)candidate.getChild(0)) );
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(1)) );
    return new OFunction(name, args);
  }
  
  private OMethod visit(MethodCallContext candidate){
    final String name = visit( ((WordContext)candidate.getChild(1)) );
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(2)) );
    return new OMethod(name, null, args);
  }
    
  private List<OExpression> visit(ArgumentsContext candidate){
    final int nbChild = candidate.getChildCount();
    final List<OExpression> elements = new ArrayList<OExpression>(nbChild);
    for(int i=1;i<nbChild-1;i+=2){
      final ParseTree child = candidate.getChild(i);
      elements.add((OExpression)visit(child));
    }
    return elements;
  }
  
}

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

import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;
import java.util.Iterator;
import java.util.LinkedHashMap;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;

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
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.command.OCommandSelect;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import com.orientechnologies.orient.core.sql.model.OLike;

import static com.orientechnologies.orient.core.sql.parser.OSQLParser.*;
import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.model.OBetween;
import com.orientechnologies.orient.core.sql.model.OOperatorDivide;
import com.orientechnologies.orient.core.sql.model.OOperatorMinus;
import com.orientechnologies.orient.core.sql.model.OOperatorModulo;
import com.orientechnologies.orient.core.sql.model.OOperatorMultiply;
import com.orientechnologies.orient.core.sql.model.OOperatorPlus;
import com.orientechnologies.orient.core.sql.model.OOperatorPower;
import com.orientechnologies.orient.core.sql.model.OPath;
import java.math.BigDecimal;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;

/**
 * Utility class to convert from SQL to a list of arguments.
 * 
 * @author Johann Sorel (Geomatys)
 */
public final class SQLGrammarUtils {

  private static ClassLoader CLASSLOADER = SQLGrammarUtils.class.getClassLoader();
  
  private SQLGrammarUtils() {
  }
  
  
  public static OCommandExecutor parse(String osql) throws OCommandSQLParsingException{
    final ParseTree tree = compileExpression(osql);
    return toOrient(tree);
  }
  
  public static ParseTree compileExpression(String osql) {
      //lexer splits input into tokens
      final CharStream input = new ANTLRInputStream(osql);
      final TokenStream tokens = new CommonTokenStream(new OSQLLexer(input));

      //parser generates abstract syntax tree
      final OSQLParser parser = new OSQLParser(tokens);
      final OSQLParser.CommandContext sentence = parser.command();
      return sentence;
  }
  
  public static OCommandExecutor toOrient(ParseTree tree) throws OCommandSQLParsingException{
    return SQLGrammarUtils.visit((OSQLParser.CommandContext)tree);
  }
  
  public static String toString(ParseTree node){
        final StringBuilder sb = new StringBuilder();
        
        if(node instanceof ParserRuleContext){
          final ParserRuleContext prc = (ParserRuleContext) node;
          sb.append("[");
          sb.append(prc.start.getStartIndex());
          sb.append(",");
          sb.append(prc.stop.getStopIndex());
          sb.append("] ");
          sb.append(prc.getRuleIndex());          
        }else if(node instanceof TerminalNode){
          final TerminalNode tn = (TerminalNode) node;
          final Token tk = tn.getSymbol();
          sb.append("[");
          sb.append(tk.getStartIndex());
          sb.append(",");
          sb.append(tk.getStopIndex());
          sb.append("] ");
          sb.append(tn.getSymbol().getType());
        }
        sb.append(" : (").append(node.getClass().getSimpleName()).append(") \"").append(node.getText()).append("\"");

        //print childrens
        final int nbChild = node.getChildCount();
        if(nbChild>0){
            sb.append('\n');
            for(int i=0;i<nbChild;i++){
                if(i==nbChild-1){
                    sb.append("\u2514\u2500 ");
                }else{
                    sb.append("\u251C\u2500 ");
                }

                String sc = toString(node.getChild(i));
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

        return sb.toString();
    }
  
  public static Object evaluate(Object candidate){
    if(candidate instanceof OExpression){
      return ((OExpression)candidate).evaluate(null, null);
    }else{
      return candidate;
    }
  }
  
  public static Object convert(Object candidate, OProperty property){
    if(candidate == null) return null;
    candidate = evaluate(candidate);
    if(candidate == null) return null;
    
    final OType type = property.getType();
    for(Class c: type.getJavaTypes()){
      if(c.isInstance(candidate)){
        //type matchs
        return candidate;
      }
    }
    
    switch(property.getType()){
      case BINARY : 
        break;
      case BOOLEAN : 
        break;
      case BYTE : 
        if(candidate instanceof Number){
          candidate = ((Number)candidate).byteValue();
        }
        break;
      case CUSTOM : 
        break;
      case DATE : 
        break;
      case DATETIME : 
        break;
      case DECIMAL : 
        if(candidate instanceof Number){
          candidate = new BigDecimal(((Number)candidate).doubleValue());
        }
        break;
      case DOUBLE : 
        if(candidate instanceof Number){
          candidate = ((Number)candidate).doubleValue();
        }
        break;
      case EMBEDDED : 
        break;
      case EMBEDDEDLIST : 
        break;
      case EMBEDDEDMAP : 
        break;
      case EMBEDDEDSET : 
        break;
      case FLOAT : 
        if(candidate instanceof Number){
          candidate = ((Number)candidate).floatValue();
        }
        break;
      case INTEGER : 
        if(candidate instanceof Number){
          candidate = ((Number)candidate).intValue();
        }
        break;
      case LINK : 
        break;
      case LINKLIST : 
        break;
      case LINKMAP : 
        break;
      case LINKSET : 
        break;
      case LONG : 
        if(candidate instanceof Number){
          candidate = ((Number)candidate).longValue();
        }
        break;
      case SHORT : 
        if(candidate instanceof Number){
          candidate = ((Number)candidate).shortValue();
        }
        break;
      case STRING : 
        candidate = candidate.toString();
        break;
      case TRANSIENT : 
        break;
    }
    return candidate;
  }
  
  
  public static <T extends ParserRuleContext> T getCommand(
          final OCommandRequest iRequest, final Class<T> c) throws OCommandSQLParsingException{
    
    final String sql = ((OCommandRequestText) iRequest).getText();
    System.err.println("|||||||||||||||||||| "+ sql);
    final ParseTree tree = compileExpression(sql);
    //System.err.println(toString(tree));
    checkErrorNodes(tree);
    if(!(tree instanceof OSQLParser.CommandContext)){
      throw new OCommandSQLParsingException("Parse error, query is not a valid command.");
    }
    
    final Object commandTree = ((OSQLParser.CommandContext)tree).getChild(0);
    if(c.isInstance(commandTree)){
      return (T) commandTree;
    }else{
      throw new OCommandSQLParsingException("Unexpected command : "+c.getClass() +" was expecting a "+c.getSimpleName());
    }
  }
  
  private static void checkErrorNodes(ParseTree tree) throws OCommandSQLParsingException {
    if(tree instanceof ErrorNode){
      throw new OCommandSQLParsingException("Malformed command at : "+tree.getText());
    }
    for(int i=0,n=tree.getChildCount();i<n;i++){
      checkErrorNodes(tree.getChild(i));
    }
  }
  
  public static OCommandExecutor visit(OSQLParser.CommandContext candidate) throws OCommandSQLParsingException {
    
    final OCommandExecutor command;
    final Object commandTree = candidate.getChild(0);
    if(commandTree instanceof OSQLParser.CommandUnknownedContext){
      command = visit((OSQLParser.CommandUnknownedContext)commandTree);
    }else{
      throw new OCommandSQLParsingException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    return command;
  }
    
  private static OCommandCustom visit(OSQLParser.CommandUnknownedContext candidate) throws OCommandSQLParsingException {
    //variables
    final List<Object> elements = new ArrayList<Object>();
    
    final int nb = candidate.getChildCount();
    for(int i=0;i<nb;i++){
      final ParseTree child = candidate.getChild(i);
      elements.add(SQLGrammarUtils.visit(child));
    }
    
    return new OCommandCustom(elements);
  }
  
  public static Object visit(ParseTree candidate) throws OCommandSQLParsingException {
    if(candidate instanceof ExpressionContext){
      return visit((ExpressionContext)candidate);
    }else if(candidate instanceof ReferenceContext){
      return visitAsExpression((ReferenceContext)candidate);
    }else if(candidate instanceof ExpressionContext){
      return visit((ExpressionContext)candidate);
    }else if(candidate instanceof LiteralContext){
      return visit((LiteralContext)candidate);
    }else if(candidate instanceof FunctionCallContext){
      return visit((FunctionCallContext)candidate);
    }else if(candidate instanceof MethodOrPathCallContext){
      return visit((MethodOrPathCallContext)candidate);
    }else if(candidate instanceof OridContext){
      return visit((OridContext)candidate);
    }else if(candidate instanceof MapContext){
      return visit((MapContext)candidate);
    }else if(candidate instanceof CollectionContext){
      return visit((CollectionContext)candidate);
    }else if(candidate instanceof UnsetContext){
      return visit((UnsetContext)candidate);
    }else if(candidate instanceof FilterContext){
      return visit((FilterContext)candidate);
    }else{
      throw new OCommandSQLParsingException("Unexpected parse tree element :"+candidate.getClass()+" "+candidate);
    }
  }
  
  public static OExpression visit(ExpressionContext candidate) throws OCommandSQLParsingException {
    final int nbChild = candidate.getChildCount();
    
    if(nbChild == 1){
      //can be a word, literal, functionCall
      return (OExpression)visit(candidate.getChild(0));
    }else if(nbChild == 2){
      //can be a method call, pathcall
      final OExpression source = (OExpression)visit(candidate.getChild(0));
      final OExpression right = (OExpression)visit(candidate.getChild(1));
      if(right instanceof OSQLMethod){
        ((OSQLMethod)right).getArguments().add(0, source); //add the source as first argument.
      }else if(right instanceof OPath){
        ((OPath)right).getChildren().add(0, source); //add the source as first argument.
      }
      return right;
    }else if(nbChild == 3){
      //can be '(' exp ')'
      //can be exp (+|-|/|*) exp
      final ParseTree left = candidate.getChild(0);
      final ParseTree center = candidate.getChild(1);
      final ParseTree right = candidate.getChild(2);
      if(center instanceof TerminalNode){
        //(+|-|/|*|.) exp
        final String operator = center.getText();
        final OExpression leftExp = (OExpression)visit(left);
        final OExpression rightExp = (OExpression)visit(right);
        if("+".equals(operator)){
          return new OOperatorPlus(leftExp, rightExp);
        }else if("-".equals(operator)){
          return new OOperatorMinus(leftExp, rightExp);
        }else if("/".equals(operator)){
          return new OOperatorDivide(leftExp, rightExp);
        }else if("*".equals(operator)){
          return new OOperatorMultiply(leftExp, rightExp);
        }else if("%".equals(operator)){
          return new OOperatorModulo(leftExp, rightExp);
        }else if("^".equals(operator)){
          return new OOperatorPower(leftExp, rightExp);
        }else{
          throw new OCommandSQLParsingException("Unexpected operator "+operator);
        }
        
      }else{
        // '(' exp ')'
        return (OExpression)visit(center);
      }
    }else{
      throw new OCommandSQLParsingException("Unexpected number of arguments");
    }
    
  }
  
  public static OName visitAsExpression(ReferenceContext candidate) throws OCommandSQLParsingException {
    return new OName(visitAsString(candidate));
  }
  
  public static String visitAsString(ReferenceContext candidate) throws OCommandSQLParsingException {
    if(candidate.WORD() != null){
      return candidate.WORD().getText();
    }else if(candidate.ESCWORD() != null){
      String txt = candidate.ESCWORD().getText();
      txt = txt.substring(1, txt.length() - 1);
      return txt;
    }else{
      String txt = candidate.keywords().getText();
      return txt;
    }
  }
  
  public static Number visit(NumberContext candidate) throws OCommandSQLParsingException {
    if (candidate.INT() != null) {
      return Integer.valueOf(candidate.getText());
    } else {
      return Double.valueOf(candidate.getText());
    }
  }
  
  public static String visit(CwordContext candidate, final OCommandRequest iRequest) throws OCommandSQLParsingException {
    if(candidate.NULL() != null){
      return null;
    }else{
      //CWord can be anything, spaces count
      String text = ((OCommandRequestText) iRequest).getText();
      return text.substring(candidate.getStart().getStartIndex());
    }
  }
  
  public static OUnset visit(UnsetContext candidate) throws OCommandSQLParsingException {
    if(candidate.UNSET() == null){
      return new OUnset(visitAsString(candidate.reference()));
    }else{
      return new OUnset();
    }
    
  }
  
  public static OLiteral visit(OridContext candidate) throws OCommandSQLParsingException {
    final ORecordId oid = new ORecordId(candidate.getText());
    return new OLiteral(oid);
  } 
  
  public static OCollection visit(CollectionContext candidate) throws OCommandSQLParsingException {
    final List col = new ArrayList();
    final List<ExpressionContext> values = candidate.expression();
    for (int i = 0, n = values.size(); i < n; i++) {
      col.add(visit(values.get(i)));
    }
    return new OCollection(col);
  }

  public static OMap visit(MapContext candidate) throws OCommandSQLParsingException {
    final LinkedHashMap map = new LinkedHashMap();
    final List<LiteralContext> keys = candidate.literal();
    final List<ExpressionContext> values = candidate.expression();
    for (int i = 0, n = keys.size(); i < n; i++) {
      map.put(visit(keys.get(i)), visit(values.get(i)));
    }
    return new OMap(map);
  }
  
  public static OLiteral visit(LiteralContext candidate) throws OCommandSQLParsingException {
    if(candidate.TEXT() != null){
      final String txt = visitText(candidate.TEXT());
      return new OLiteral(txt);
      
    }else if(candidate.number()!= null){
      return new OLiteral(visit(candidate.number()));
      
    }else if(candidate.NULL()!= null){
      return new OLiteral(null);
      
    }else{
      throw new OCommandSQLParsingException("Should not happen");
    }
  }
  
  public static String visitText(TerminalNode candidate) {
    String txt = candidate.getText();
    txt = txt.substring(1, txt.length() - 1);
    return txt;
  }

  public static OSQLFunction visit(FunctionCallContext candidate) throws OCommandSQLParsingException {
    final String name = visitAsString((ReferenceContext)candidate.getChild(0));
    final List<OExpression> args = visit( ((ArgumentsContext)candidate.getChild(1)) );
    final OSQLFunction fct = createFunction(name);
    fct.getArguments().addAll(args);
    return fct;
  }
  
  public static OExpression visit(MethodOrPathCallContext candidate) throws OCommandSQLParsingException {
    if(candidate.arguments() != null){
      final String name = visitAsString(candidate.reference());
      final List<OExpression> args = visit(candidate.arguments());
      final OSQLMethod method = createMethod(name);
      method.getArguments().addAll(args);
      return method;
    }else{
      final OName name = visitAsExpression(candidate.reference());
      final OPath path = new OPath(null,null,null);
      path.getChildren().clear();
      path.getChildren().add(name);
      return path;
    }
    
  }

  public static OExpression visit(ProjectionContext candidate) throws OCommandSQLParsingException {
    
    OExpression exp;
    if(candidate.filter() != null){
      exp = visit(candidate.filter());
    }else if(candidate.expression()!= null){
      exp = visit(candidate.expression());
    }else{
      throw new OCommandSQLParsingException("Unknowned command " + candidate.getClass()+" "+candidate);
    }
    
    if(candidate.alias() != null){
      exp.setAlias(visitAsString(candidate.alias().reference()));
    }
    
    return exp;
  }
  
  public static OExpression visit(FilterContext candidate) throws OCommandSQLParsingException {
    final int nbChild = candidate.getChildCount();
    if(nbChild == 1){
      //can be a word, literal, functionCall
      return (OExpression) visit(candidate.getChild(0));
    }else if(nbChild == 2){
      //can be :
      //filter filterAnd
      //filter filterOr
      //expression filterIn
      //expression filterBetween
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
          throw new OCommandSQLParsingException("Unexpected arguments");
        }
        return new OIn(left,right);
      }else if(candidate.filterBetween()!= null){
        final OExpression target = (OExpression)visit(candidate.getChild(0));
        final OExpression left = (OExpression)visit(candidate.filterBetween().expression(0));
        final OExpression right = (OExpression)visit(candidate.filterBetween().expression(1));
        return new OBetween(target,left,right);
      }else if(candidate.NOT() != null){
        return new ONot(
                (OExpression)visit(candidate.getChild(1)));
      }else{
        throw new OCommandSQLParsingException("Unexpected arguments");
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
      throw new OCommandSQLParsingException("Unexpected number of arguments");
    }
  }
  
  public static List<OExpression> visit(ArgumentsContext candidate) throws OCommandSQLParsingException {
    final int nbChild = candidate.getChildCount();
    final List<OExpression> elements = new ArrayList<OExpression>(nbChild);
    for(int i=1;i<nbChild-1;i+=2){
      final ParseTree child = candidate.getChild(i);
      elements.add((OExpression)visit(child));
    }
    return elements;
  }
    
  public static List<ORID> visit(OSQLParser.SourceContext candidate) throws OCommandSQLParsingException {
    List<ORID> ids = new ArrayList<ORID>();
    if(candidate.orid() != null){
      //single identifier
      final OLiteral literal = visit(candidate.orid());
      final OIdentifiable id = (OIdentifiable) literal.evaluate(null, null);
      ids.add(id.getIdentity());
      
    }else if(candidate.collection() != null){
      //collection of identifier
      final OCollection col = visit(candidate.collection());
      List lst = col.evaluate(null, null);
      for(Object obj : lst){
        ids.add( ((ORecordId)obj).getIdentity() );
      }
      
    }else if(candidate.commandSelect() != null){
      //sub query
      final OCommandSelect sub = new OCommandSelect();
      sub.parse(candidate.commandSelect());
      for(Object obj : sub){
        ids.add( ((ORecordId)obj).getIdentity() );
      }
    }
    return ids;
  }
  
  public static OSQLMethod createMethod(String name) throws OCommandSQLParsingException{
    final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, CLASSLOADER);
    while (ite.hasNext()) {
      final OSQLMethodFactory factory = ite.next();
      if (factory.hasMethod(name)) {
        return factory.createMethod(name);
      }
    }
    throw new OCommandSQLParsingException("No method for name : "+name);
  }
  
  public static OSQLFunction createFunction(String name) throws OCommandSQLParsingException{
    final Iterator<OSQLFunctionFactory> ite = lookupProviderWithOrientClassLoader(OSQLFunctionFactory.class, CLASSLOADER);
    while (ite.hasNext()) {
      final OSQLFunctionFactory factory = ite.next();
      if (factory.hasFunction(name)) {
        return factory.createFunction(name);
      }
    }
    throw new OCommandSQLParsingException("No function for name : "+name);
  }
  
}

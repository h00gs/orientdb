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
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.model.OExpression;
import java.math.BigDecimal;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Utility class to convert from SQL to a list of arguments.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OSQL {
  
  public static OCommandExecutor parse(String osql){
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

  public static OCommandExecutor toOrient(ParseTree tree){
    final AntlrToOrientVisitor visitor = new AntlrToOrientVisitor();
    return visitor.visit((OSQLParser.CommandContext)tree);
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
  
}

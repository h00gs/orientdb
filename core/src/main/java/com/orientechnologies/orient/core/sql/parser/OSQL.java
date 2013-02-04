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

import java.util.List;
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
  
  public static List<Object> parse(String osql){
    final ParseTree tree = compileExpression(osql);
    return toOrient(tree);
  }
  
  public static ParseTree compileExpression(String osql) {
      //lexer splits input into tokens
      final CharStream input = new ANTLRInputStream(osql);
      final TokenStream tokens = new CommonTokenStream(new OSQLLexer(input));

      //parser generates abstract syntax tree
      final OSQLParser parser = new OSQLParser(tokens);
      final OSQLParser.SentenceContext sentence = parser.sentence();
      return sentence;
  }

  public static List<Object> toOrient(ParseTree tree){
    final AntlrToOrientVisitor visitor = new AntlrToOrientVisitor();
    return visitor.visit((OSQLParser.SentenceContext)tree);
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
  
}

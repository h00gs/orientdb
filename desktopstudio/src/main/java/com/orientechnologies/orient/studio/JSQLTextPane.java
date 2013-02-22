/*
 * Copyright 2013, Geomatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.studio;

import com.orientechnologies.orient.core.sql.parser.OSQLLexer;
import com.orientechnologies.orient.core.sql.parser.OSQLParser.OridContext;
import com.orientechnologies.orient.core.sql.parser.SQLGrammarUtils;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextPane;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * OrientDB SQL text pane. highlights syntax.
 *
 * @author Johann Sorel (Geomatys)
 */
public class JSQLTextPane extends JPanel implements KeyListener {

  private final JTextPane guiText = new JTextPane();
  private final JLabel guiError = new JLabel();
  final Style styleDefault;
  final Style styleKeyWords;
  final Style styleLiteral;
  final Style styleSyntax;
  final Style styleDynamic;
  final Style styleWord;
  final Style styleError;

  public JSQLTextPane() {
    super(new BorderLayout());

    guiText.setBackground(Color.WHITE);

    add(BorderLayout.CENTER, new JScrollPane(guiText));
    add(BorderLayout.SOUTH, guiError);
    guiText.addKeyListener(this);

    styleDefault = guiText.addStyle("default", null);
    StyleConstants.setForeground(styleDefault, Color.BLACK);

    styleKeyWords = guiText.addStyle("keyword", null);
    StyleConstants.setForeground(styleKeyWords, Color.GRAY);
    StyleConstants.setBold(styleKeyWords, true);

    styleLiteral = guiText.addStyle("literal", null);
    StyleConstants.setForeground(styleLiteral, new Color(0, 150, 0));

    styleSyntax = guiText.addStyle("syntax", null);
    StyleConstants.setForeground(styleSyntax, Color.GRAY);
    StyleConstants.setBold(styleSyntax, true);

    styleDynamic = guiText.addStyle("dynamic", null);
    StyleConstants.setForeground(styleDynamic, Color.BLUE);
    StyleConstants.setBold(styleDynamic, true);
    
    styleWord = guiText.addStyle("word", null);
    StyleConstants.setForeground(styleWord, Color.MAGENTA);
    StyleConstants.setBold(styleWord, false);

    styleError = guiText.addStyle("error", null);
    StyleConstants.setForeground(styleError, Color.RED);
    StyleConstants.setBold(styleError, true);

  }

  public void setText(String cql) {
    guiText.setText(cql);
    updateHightLight();
  }

  /**
   * Insert text at current caret position
   *
   * @param text
   */
  public void insertText(String text) {
    final int position = guiText.getCaretPosition();
    final String cql = guiText.getText();
    final StringBuilder sb = new StringBuilder();
    sb.append(cql.substring(0, position));
    sb.append(text);
    sb.append(cql.substring(position));

    guiText.setText(sb.toString());
    guiText.setCaretPosition(position + text.length());
    updateHightLight();
  }

  public void addText(String text) {
    guiText.setText(guiText.getText() + text);
    updateHightLight();
  }

  public String getText() {
    return guiText.getText();
  }

  @Override
  public void keyTyped(KeyEvent e) {
  }

  @Override
  public void keyPressed(KeyEvent e) {
  }

  @Override
  public void keyReleased(KeyEvent e) {
    updateHightLight();
  }

  private void updateHightLight() {
    final StyledDocument doc = (StyledDocument) guiText.getDocument();
    final String txt = guiText.getText();

    final ParseTree tree = SQLGrammarUtils.compileExpression(txt);
    doc.setCharacterAttributes(0, txt.length(), styleDefault, true);
    syntaxHighLight(tree, doc);
  }

  private void syntaxHighLight(ParseTree tree, StyledDocument doc) {

    if (tree instanceof ErrorNode) {
      final ErrorNode en = (ErrorNode) tree;
      final int start = en.getSymbol().getStartIndex();
      final int end = en.getSymbol().getStopIndex();
      doc.setCharacterAttributes(start, end, styleError, true);
    }else if (tree instanceof OridContext) {
      final OridContext en = (OridContext) tree;
      final int start = en.getStart().getStartIndex();
      final int end = en.getStop().getStopIndex();
      doc.setCharacterAttributes(start, end, styleDynamic, true);
      //do not explore children
      return;
    } else if (tree instanceof TerminalNode) {
      final TerminalNode tn = (TerminalNode) tree;
      final int type = tn.getSymbol().getType();
      final int start = tn.getSymbol().getStartIndex();
      final int end = tn.getSymbol().getStopIndex()+1;
      
      final Style style;
      switch(type){
        //keywords
        case OSQLLexer.SELECT :
        case OSQLLexer.INSERT :
         case OSQLLexer.UPDATE :
        case OSQLLexer.CREATE :
        case OSQLLexer.DELETE :
        case OSQLLexer.FROM :
        case OSQLLexer.WHERE :
        case OSQLLexer.INTO :
        case OSQLLexer.DROP :
        case OSQLLexer.FORCE :
        case OSQLLexer.VALUES :
        case OSQLLexer.SET :
        case OSQLLexer.ADD :
        case OSQLLexer.REMOVE :
        case OSQLLexer.AND :
        case OSQLLexer.OR :
        case OSQLLexer.ORDER :
        case OSQLLexer.BY :
        case OSQLLexer.LIMIT :
        case OSQLLexer.LIKE :
        case OSQLLexer.RANGE :
        case OSQLLexer.ASC :
        case OSQLLexer.AS :
        case OSQLLexer.DESC :
        case OSQLLexer.CLUSTER :
        case OSQLLexer.DATABASE :
        case OSQLLexer.PROPERTY :
        case OSQLLexer.TRUNCATE :
        case OSQLLexer.EXTENDS :
        case OSQLLexer.ABSTRACT :
        case OSQLLexer.RECORD :
        case OSQLLexer.INDEX :
        case OSQLLexer.DICTIONARY :
        case OSQLLexer.ALTER :
        case OSQLLexer.CLASS :
        case OSQLLexer.SKIP :
        case OSQLLexer.GRANT :
        case OSQLLexer.REVOKE :
        case OSQLLexer.IN :
        case OSQLLexer.ON :
        case OSQLLexer.TO :
        case OSQLLexer.IS :
        case OSQLLexer.NOT :
        case OSQLLexer.GROUP :
        case OSQLLexer.DATASEGMENT :
        case OSQLLexer.LOCATION :
        case OSQLLexer.POSITION :
        case OSQLLexer.RUNTIME :
        case OSQLLexer.EDGE :
        case OSQLLexer.FUNCTION :
        case OSQLLexer.LINK :
        case OSQLLexer.VERTEX :
        case OSQLLexer.TYPE :
        case OSQLLexer.INVERSE :
        case OSQLLexer.IDEMPOTENT :
        case OSQLLexer.LANGUAGE :
        case OSQLLexer.FIND :
        case OSQLLexer.REFERENCES :
        case OSQLLexer.REBUILD :
        case OSQLLexer.TRAVERSE :
        case OSQLLexer.PUT :
        case OSQLLexer.INCREMENT :
        case OSQLLexer.WHILE :
        case OSQLLexer.BETWEEN :
          style = styleKeyWords;
          break;
        //syntax
        case OSQLLexer.COMMA :
        case OSQLLexer.DOUBLEDOT :
        case OSQLLexer.DOT :
        case OSQLLexer.MULT :
        case OSQLLexer.COMPARE_EQL :
        case OSQLLexer.COMPARE_DIF :
        case OSQLLexer.COMPARE_INF :
        case OSQLLexer.COMPARE_INF_EQL :
        case OSQLLexer.COMPARE_SUP :
        case OSQLLexer.COMPARE_SUP_EQL :
        case OSQLLexer.LPAREN :
        case OSQLLexer.RPAREN :
        case OSQLLexer.LBRACKET :
        case OSQLLexer.RBRACKET :
        case OSQLLexer.LACCOLADE :
        case OSQLLexer.RACCOLADE :
          style = styleSyntax;
          break;
        //literal
        case OSQLLexer.UNARY :
        case OSQLLexer.TEXT :
        case OSQLLexer.INT :
        case OSQLLexer.FLOAT :
          style = styleLiteral;
          break;
        //dynamics
        case OSQLLexer.OTHIS :
        case OSQLLexer.ORID_ATTR:
        case OSQLLexer.OCLASS_ATTR:
        case OSQLLexer.OVERSION_ATTR:
        case OSQLLexer.OSIZE_ATTR:
        case OSQLLexer.OTYPE_ATTR:
        case OSQLLexer.UNSET :
        case OSQLLexer.NULL :
        case OSQLLexer.ORID :
          style = styleDynamic;
          break;
        //unknowned
        case OSQLLexer.ESCWORD :
        case OSQLLexer.WORD :
          style = styleWord;
          break;
        //not visible
        case OSQLLexer.WS :
        default :
          style = styleDefault;
          break;
      }
      doc.setCharacterAttributes(start, end, style, true);
    }

    final int nbchild = tree.getChildCount();
    for (int i = 0; i < nbchild; i++) {
      syntaxHighLight((ParseTree) tree.getChild(i), doc);
    }
  }
}

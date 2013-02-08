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
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.command.OCommandCustom;
import com.orientechnologies.orient.core.sql.command.OCommandInsert;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.model.OCollection;
import com.orientechnologies.orient.core.sql.model.OMap;
import com.orientechnologies.orient.core.sql.model.OName;
import com.orientechnologies.orient.core.sql.model.OUnset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.antlr.v4.runtime.tree.ParseTree;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 * Test ANTLR4 SQL Parser
 * 
 * @author Johann Sorel (Geomatys)
 */
public class SQLParserTest {
  
  @Test
  public void testWord() throws SyntaxException{
    final String sql = "hello world";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),2);
    assertEquals(objs.get(0), new OName("hello"));
    assertEquals(objs.get(1), new OName("world"));
  }
  
  @Test
  public void testLiteralText1() throws SyntaxException{
    final String sql = "hello \"world\"";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),2);
    assertEquals(objs.get(0),new OName("hello"));
    assertEquals(objs.get(1),new OLiteral("world"));
  }
  
  @Test
  public void testLiteralText2() throws SyntaxException{
    final String sql = "hello 'world'";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),2);
    assertEquals(objs.get(0),new OName("hello"));
    assertEquals(objs.get(1),new OLiteral("world"));
  }
  
  @Test
  public void testLiteralText3() throws SyntaxException{
    final String sql = "hello 'world'') test'";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),2);
    assertEquals(objs.get(0),new OName("hello"));
    assertEquals(objs.get(1),new OLiteral("world'') test"));
  }
  
  @Test
  public void testLiteralNumber() throws SyntaxException{
    String sql = "2013";
    OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    List<Object> objs = command.getArguments();
    assertEquals(objs.size(),1);
    assertEquals(objs.get(0), new OLiteral(2013));
    
    sql = "-2013";
    command = (OCommandCustom) OSQL.parse(sql);
     objs = command.getArguments();
    assertEquals(objs.size(),1);
    assertEquals(objs.get(0), new OLiteral(-2013));
    
    sql = "-2013e3";
    command = (OCommandCustom) OSQL.parse(sql);
    objs = command.getArguments();
    assertEquals(objs.size(),1);
    assertEquals(objs.get(0), new OLiteral(-2013e3d));
    
    sql = "-2013.456e-3";
    command = (OCommandCustom) OSQL.parse(sql);
    objs = command.getArguments();
    assertEquals(objs.size(),1);
    assertEquals(objs.get(0), new OLiteral(-2013.456e-3d));
  }
  
  @Test
  public void testLiteralNull() throws SyntaxException{
    final String sql = "hello null";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),2);
    assertEquals(objs.get(0),new OName("hello"));
    assertEquals(objs.get(1),new OLiteral(null));
  }
  
  @Test
  public void testUnset() throws SyntaxException{
    final String sql = "hello ?";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),2);
    assertEquals(objs.get(0),new OName("hello"));
    assertEquals(objs.get(1),new OUnset());
  }
  
  @Test
  public void testCollection() throws SyntaxException{
    final String sql = "[123,'abc',\"def\"]";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),1);
    final OCollection lit = (OCollection) objs.get(0);
    final List lst = lit.getChildren();
    assertEquals(lst.get(0),new OLiteral(123));
    assertEquals(lst.get(1),new OLiteral("abc"));
    assertEquals(lst.get(2),new OLiteral("def"));
  }
  
  @Test
  public void testMap() throws SyntaxException{
    final String sql = "{'att1':123,'att2':{'satt1':456}}";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),1);
    final OMap lit = (OMap) objs.get(0);
    final Map lst = lit.getMap();
    assertEquals(lst.size(), 2);
    final Iterator<Entry> ite = lst.entrySet().iterator();
    final Entry entry1 = ite.next();
    final Entry entry2 = ite.next();
    assertEquals(entry1.getKey(),new OLiteral("att1"));
    assertEquals(entry1.getValue(),new OLiteral(123));
    assertEquals(entry2.getKey(),new OLiteral("att2"));
    final OMap v2 = (OMap) entry2.getValue();
    assertEquals(v2.getMap().size(), 1);
    final Entry sentry1 = v2.getMap().entrySet().iterator().next();
    assertEquals(sentry1.getKey(),new OLiteral("satt1"));
    assertEquals(sentry1.getValue(),new OLiteral(456));
  }
  
//  @Test
//  public void testFunction() throws SyntaxException{
//    final String sql = "max( 4 , 36 , 21 )";
//    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
//    final List<Object> objs = command.getArguments();
//    assertEquals(objs.size(),1);
//    
//    final OSQLFunction fct = SQLGrammarUtils.createFunction("max");
//    fct.getArguments().add(new OLiteral(4));
//    fct.getArguments().add(new OLiteral(36));
//    fct.getArguments().add(new OLiteral(21));
//    
//    assertEquals(objs.get(0),fct);
//  }
  
  @Test
  public void testMethodCall() throws SyntaxException{
    final String sql = "'text'.charAt(2)";
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),1);
    final OSQLMethod m = SQLGrammarUtils.createMethod("charAt");
    m.getArguments().add(new OLiteral("text"));
    m.getArguments().add(new OLiteral(2));
    assertEquals(objs.get(0),m);
    
    //test method call
    final Object obj = ((OExpression)objs.get(0)).evaluate(null, null);
    assertEquals(obj, "x");
    
  }
  
  @Test
  public void testMethodStackCall() throws SyntaxException{
    final String sql = "'t213t'.substring(1,4).asInteger()";    
    final OCommandCustom command = (OCommandCustom) OSQL.parse(sql);
    final List<Object> objs = command.getArguments();
    assertEquals(objs.size(),1);
    
    final OSQLMethod sm = SQLGrammarUtils.createMethod("substring");
    sm.getArguments().add(new OLiteral("t213t"));
    sm.getArguments().add(new OLiteral(1));
    sm.getArguments().add(new OLiteral(4));
    
    final OSQLMethod m = SQLGrammarUtils.createMethod("asInteger");
    m.getArguments().add(sm);
        
    assertEquals(objs.get(0),m);
    
    //test method call
    final Object obj = ((OExpression)objs.get(0)).evaluate(null, null);
    assertEquals(obj, 213);
  }
  
  public void testInsertIntoByValues(){
    final String sql = "INSERT INTO table(att1,att2,att3) VALUES ('a','b',3), ('d','e',6)";
    OCommandInsert command = new OCommandInsert();
    command.parse(new OCommandSQL(sql));
    assertEquals(command.getTarget(),
            "table");
    assertEquals(command.getFields(),
            new String[]{"att1","att2","att3"} );
    assertEquals(command.getRecords(),
            Arrays.asList(
                new Object[]{new OLiteral("a"),new OLiteral("b"),new OLiteral(3)},
                new Object[]{new OLiteral("d"),new OLiteral("e"),new OLiteral(6)}) 
            );
    
  }
  
  public void testInsertIntoBySet(){
    final String sql = "INSERT INTO table SET att1='a',att2='b',att3=3";
    OCommandInsert command = new OCommandInsert();
    command.parse(new OCommandSQL(sql));
    assertEquals(command.getTarget(),
            "table");
    assertEquals(command.getFields(),
            new String[]{"att1","att2","att3"} );
    assertEquals(command.getRecords(),
            Collections.singletonList(
                new Object[]{new OLiteral("a"),new OLiteral("b"),new OLiteral(3)}) 
            );
    
  }
  
  public void testInsertIntoBySubQuery(){
    final String sql = "INSERT INTO test SET names = (select name from OUser)";
    
  }
  
}

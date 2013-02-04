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
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OFunction;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OMethod;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

/**
 * Test ANTLR4 SQL Parser
 * 
 * @author Johann Sorel (Geomatys)
 */
public class SQLParserTest {
  
  @Test
  public void testWord(){
    final String sql = "hello world";
    final List<Object> objs = OSQL.parse(sql);
    assertEquals(2, objs.size());
    assertEquals("hello", objs.get(0));
    assertEquals("world", objs.get(1));
  }
  
  @Test
  public void testLiteralText1(){
    final String sql = "hello \"world\"";
    final List<Object> objs = OSQL.parse(sql);
    assertEquals(2, objs.size());
    assertEquals("hello", objs.get(0));
    assertEquals(new OLiteral("world"), objs.get(1));
  }
  
  @Test
  public void testLiteralText2(){
    final String sql = "hello 'world'";
    final List<Object> objs = OSQL.parse(sql);
    assertEquals(2, objs.size());
    assertEquals("hello", objs.get(0));
    assertEquals(new OLiteral("world"), objs.get(1));
  }
  
  @Test
  public void testLiteralNumber(){
    String sql = "2013";
    List<Object> objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    assertEquals(new OLiteral(2013d), objs.get(0));
    
    sql = "-2013";
    objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    assertEquals(new OLiteral(-2013d), objs.get(0));
    
    sql = "-2013e3";
    objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    assertEquals(new OLiteral(-2013e3d), objs.get(0));
    
    sql = "-2013.456e-3";
    objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    assertEquals(new OLiteral(-2013.456e-3d), objs.get(0));
  }
  
  @Test
  public void testFunction(){
    final String sql = "call( 4 , \"sometext\" )";
    final List<Object> objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    assertEquals(new OFunction("call", Arrays.asList((OExpression)new OLiteral(4d), new OLiteral("sometext"))),
               objs.get(0));
  }
  
  @Test
  public void testMethodCall(){
    final String sql = "'text'.charAt(3)";
    final List<Object> objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    assertEquals(new OMethod("charAt", 
                  (OExpression)new OLiteral("text"), 
                  Arrays.asList(
                    (OExpression)new OLiteral(3d))),
               objs.get(0));
  }
  
  @Test
  public void testMethodStackCall(){
    final String sql = "'text'.charAt(3).toString()";
    final List<Object> objs = OSQL.parse(sql);
    assertEquals(1, objs.size());
    
    OMethod sm = new OMethod("charAt", 
                  (OExpression)new OLiteral("text"), 
                  Arrays.asList(
                    (OExpression)new OLiteral(3d)));
    OMethod m = new OMethod("toString", 
                  (OExpression)sm, 
                  new ArrayList<OExpression>());
    
    assertEquals(m, objs.get(0));
  }
  
  
  
  
  
}

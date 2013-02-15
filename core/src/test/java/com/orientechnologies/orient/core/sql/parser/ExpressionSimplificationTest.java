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

import com.orientechnologies.orient.core.sql.model.OAnd;
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OName;
import com.orientechnologies.orient.core.sql.model.ONotEquals;
import com.orientechnologies.orient.core.sql.model.OOperatorDivide;
import com.orientechnologies.orient.core.sql.model.OOperatorMultiply;
import com.orientechnologies.orient.core.sql.model.OOperatorPlus;
import com.orientechnologies.orient.core.sql.model.OOr;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public class ExpressionSimplificationTest {
  
  @Test
  public void and1Test(){    
    final OExpression equals = new OEquals(new OLiteral("test"), new OLiteral("test"));
    final OExpression notequals = new ONotEquals(new OLiteral(14), new OLiteral(21));    
    final OExpression and = new OAnd(equals, notequals);
    
    final OExpression simple = (OExpression) and.accept(OSimplifyVisitor.INSTANCE, null);
    //both condition are always true
    assertEquals(simple, OExpression.INCLUDE);
  }
  
  @Test
  public void and2Test(){    
    final OExpression equals = new OEquals(new OLiteral("test"), new OLiteral("test456"));
    final OExpression notequals = new ONotEquals(new OLiteral(14), new OLiteral(21));    
    final OExpression and = new OAnd(equals, notequals);
    
    final OExpression simple = (OExpression) and.accept(OSimplifyVisitor.INSTANCE, null);
    //first condition is always false
    assertEquals(simple, OExpression.EXCLUDE);
  }
  
  @Test
  public void or1Test(){    
    final OExpression equals = new OEquals(new OLiteral("test"), new OLiteral("test"));
    final OExpression notequals = new ONotEquals(new OLiteral(14), new OLiteral(21));    
    final OExpression or = new OOr(equals, notequals);
    
    final OExpression simple = (OExpression) or.accept(OSimplifyVisitor.INSTANCE, null);
    //both condition are always true
    assertEquals(simple, OExpression.INCLUDE);
  }
    
  @Test
  public void or2Test(){    
    final OExpression equals = new OEquals(new OLiteral("test"), new OLiteral("test456"));
    final OExpression notequals = new ONotEquals(new OLiteral(14), new OName("att"));    
    final OExpression or = new OOr(equals, notequals);
    
    final OExpression simple = (OExpression) or.accept(OSimplifyVisitor.INSTANCE, null);
    //first condition is always false, simplefied in only second condition
    assertEquals(simple, notequals);
  }
  
  @Test
  public void mathTest(){    
    final OExpression add = new OOperatorPlus(new OLiteral(45), new OLiteral(5));
    final OExpression mul = new OOperatorMultiply(add, new OLiteral(10));    
    final OExpression div = new OOperatorDivide(mul,new OLiteral(5));    
    
    final OExpression simple = (OExpression) div.accept(OSimplifyVisitor.INSTANCE, null);
    //second condition is always true
    assertEquals(simple, new OLiteral(100d));
  }
  
}

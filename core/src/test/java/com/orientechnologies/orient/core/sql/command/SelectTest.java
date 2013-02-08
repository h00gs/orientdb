/*
 * Copyright 2013 Orient Technologies.
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
package com.orientechnologies.orient.core.sql.command;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Test SQL Select command.
 *
 * @author Johann Sorel (Geomatys)
 */
@Test
public class SelectTest {

  private static File folder;
  static {
    try {
      folder = File.createTempFile("orientdb", "");
      folder.delete();
      folder.mkdirs();
    } catch (IOException ex) {
    }
  }

  private ODatabaseDocumentTx db;
  
  @BeforeMethod
  public void initMethod(){
    db = new ODatabaseDocumentTx("local:"+folder.getPath());
    db = db.open("admin", "admin");
  }
    
  @AfterMethod
  public void endMethod(){
    if(db != null){
      db.close();
    }
  }
  
  public SelectTest(){
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:"+folder.getPath());
    db = db.create();
    
    final OSchema schema = db.getMetadata().getSchema();
    final OClass clazz = schema.createClass("Car");
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("size", OType.DOUBLE);
    
    final ODocument car1 = db.newInstance(clazz.getName());
    car1.field("name","tempo");
    car1.field("size",250);
    car1.save();
    
    final ODocument car2 = db.newInstance(clazz.getName());
    car2.field("name","fiesta");
    car2.field("size",160);
    car2.save();
    
    final ODocument car3 = db.newInstance(clazz.getName());
    car3.field("size",260);
    car3.save();
    
    final ODocument car4 = db.newInstance(clazz.getName());
    car4.field("name","supreme");
    car4.field("size",310);
    car4.save();
    
    db.close();
  }
  
  @Test
  public void selectAll1(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4 );
    assertEquals(docs.get(0).fieldNames().length, 2);
  }
  
  @Test
  public void selectAll2(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT * FROM car");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4 );
    assertEquals(docs.get(0).fieldNames().length, 2);
  }
  
  @Test
  public void selectField(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT name FROM car");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4);
    assertEquals(docs.get(0).fieldNames().length, 1);
    assertEquals(docs.get(0).field("0"), "tempo");
    assertEquals(docs.get(1).field("0"), "fiesta");
    assertEquals(docs.get(2).field("0"), null);
    assertEquals(docs.get(3).field("0"), "supreme");
  }
  
  @Test
  public void selectAlias(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT name AS brand FROM car");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4);
    assertEquals(docs.get(0).fieldNames().length, 1);
    assertEquals(docs.get(0).field("brand"), "tempo");
    assertEquals(docs.get(1).field("brand"), "fiesta");
    assertEquals(docs.get(2).field("brand"), null);
    assertEquals(docs.get(3).field("brand"), "supreme");
  }
  
  @Test
  public void selectLimit(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car LIMIT 2");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 2);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "tempo");
    assertEquals(docs.get(1).field("name"), "fiesta");
  }
  
  @Test
  public void selectSkip(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car SKIP 1 LIMIT 1");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "fiesta");
  }
  
  @Test
  public void selectMethod(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT size.asInteger().append(' length'), name.charAt(2) AS letter FROM car");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("letter"), "m");
    assertEquals(docs.get(1).field("letter"), "e");
    assertEquals(docs.get(2).field("letter"), null);
    assertEquals(docs.get(3).field("letter"), "p");
    assertEquals(docs.get(0).field("0"), "250 length");
    assertEquals(docs.get(1).field("0"), "160 length");
    assertEquals(docs.get(2).field("0"), "260 length");
    assertEquals(docs.get(3).field("0"), "310 length");
  }
  
  @Test
  public void selectFromSubQuery(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM (SELECT FROM car SKIP 1 LIMIT 1)");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "fiesta");
  }
  
  @Test
  public void selectFromSingleId(){    
    OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car SKIP 1 LIMIT 1");
    List<ODocument> docs = db.query(query);
    String id = docs.get(0).getIdentity().toString();
    
    query = new OSQLSynchQuery("SELECT FROM "+id);
    docs = db.query(query);    
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "fiesta");
  }
  
  @Test
  public void selectFromMultipleIds(){    
    OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car SKIP 1");
    List<ODocument> docs = db.query(query);
    String id1 = docs.get(0).getIdentity().toString();
    String id2 = docs.get(1).getIdentity().toString();
    String id3 = docs.get(2).getIdentity().toString();
    
    query = new OSQLSynchQuery("SELECT FROM ["+id1+","+id2+","+id3+"]");
    docs = db.query(query);    
    assertEquals(docs.size(), 3);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "fiesta");
    assertEquals(docs.get(1).field("name"), null);
    assertEquals(docs.get(2).field("name"), "supreme");
  }
  
  @Test
  public void selectWhereBasic(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car WHERE size > 200");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 3);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "tempo");
    assertEquals(docs.get(1).field("name"), null);
    assertEquals(docs.get(2).field("name"), "supreme");
  }
  
  @Test
  public void selectWhereComplex(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car WHERE name IS NOT NULL AND (size < 200 OR name = 'tempo')");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 2);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "tempo");
    assertEquals(docs.get(1).field("name"), "fiesta");
  }
  
}

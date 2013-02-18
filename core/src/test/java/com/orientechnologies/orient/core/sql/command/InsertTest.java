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
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class InsertTest {

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
      db.command(new OCommandSQL("DELETE FROM car")).execute();
      db.command(new OCommandSQL("DELETE FROM person")).execute();
      db.command(new OCommandSQL("DELETE FROM fish")).execute();
      db.command(new OCommandSQL("DELETE FROM boat")).execute();
      db.command(new OCommandSQL("DELETE FROM dock")).execute();
      db.command(new OCommandSQL("DELETE FROM sea")).execute();
      db.close();
    }
  }
  
  public InsertTest(){
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:"+folder.getPath());
    db = db.create();
    
    final OSchema schema = db.getMetadata().getSchema();
    
    //car test type
    final OClass carClass = schema.createClass("Car");
    carClass.createProperty("name", OType.STRING);
    carClass.createProperty("size", OType.DOUBLE);    
    
    //person test type
    final OClass personClass = schema.createClass("person");
    personClass.createProperty("name", OType.STRING);    
    personClass.createProperty("size", OType.DOUBLE);    
    personClass.createProperty("weight", OType.DOUBLE);    
    personClass.createProperty("points", OType.INTEGER);    
    
    //complex test type
    final OClass fishClass = schema.createClass("fish");
    fishClass.createProperty("name", OType.STRING);
    final OClass boatClass = schema.createClass("boat");
    boatClass.createProperty("name", OType.STRING);
    boatClass.createProperty("freight", OType.EMBEDDED,fishClass);
    final OClass dockClass = schema.createClass("dock");
    dockClass.createProperty("name", OType.STRING);
    dockClass.createProperty("capacity", OType.DOUBLE);
    final OClass seaClass = schema.createClass("sea");
    seaClass.createProperty("name", OType.STRING);
    seaClass.createProperty("navigator", OType.EMBEDDED,boatClass);
    seaClass.createProperty("docks", OType.EMBEDDEDLIST,dockClass);
        
    db.close();
  }
  
  @Test
  public void insertSingle(){
    final OCommandSQL query = new OCommandSQL("INSERT INTO car(name,size) VALUES ('tempo',250)");
    db.command(query).execute();
    final List<ODocument> docs = db.query(new OSQLSynchQuery("SELECT FROM car"));
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).fieldNames().length, 2);
  }
  
  @Test
  public void insertEscapeSequences(){
    final OCommandSQL query = new OCommandSQL("INSERT INTO \"car\"(\"name\",\"size\") VALUES ('tem''po',250)");
    db.command(query).execute();
    final List<ODocument> docs = db.query(new OSQLSynchQuery("SELECT FROM car"));
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "tem'po");
    assertEquals(docs.get(0).field("size"), 250d);
  }
  
  @Test
  public void insertMultiple(){
    final OCommandSQL query = new OCommandSQL("INSERT INTO car(name,size) VALUES ('tempo',250),('fiesta',160),(null,260),('supreme',310)");
    db.command(query).execute();
    final List<ODocument> docs = db.query(new OSQLSynchQuery("SELECT FROM car"));
    assertEquals(docs.size(), 4);
    assertEquals(docs.get(0).fieldNames().length, 2);
  }
  
  
}

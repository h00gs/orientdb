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
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.testng.Assert.*;

/**
 * Test SQL Traverse command.
 *
 * @author Johann Sorel (Geomatys)
 */
@Test
public class TraverseTest {

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

  public TraverseTest(){
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:"+folder.getPath());
    db = db.create();
    
    final OSchema schema = db.getMetadata().getSchema();
    
    //car test type
    final OClass carClass = schema.createClass("Car");
    carClass.createProperty("name", OType.STRING);
    carClass.createProperty("size", OType.DOUBLE);    
    final ODocument car1 = db.newInstance(carClass.getName());
    car1.field("name","tempo");
    car1.field("size",250);
    car1.save();    
    final ODocument car2 = db.newInstance(carClass.getName());
    car2.field("name","fiesta");
    car2.field("size",160);
    car2.save();    
    final ODocument car3 = db.newInstance(carClass.getName());
    car3.field("size",260);
    car3.save();    
    final ODocument car4 = db.newInstance(carClass.getName());
    car4.field("name","supreme");
    car4.field("size",310);
    car4.save();
    
    //person test type
    final OClass personClass = schema.createClass("person");
    personClass.createProperty("name", OType.STRING);    
    personClass.createProperty("size", OType.DOUBLE);    
    personClass.createProperty("weight", OType.DOUBLE);    
    personClass.createProperty("points", OType.INTEGER);   
    personClass.createProperty("alive", OType.BOOLEAN);    
    final ODocument person1 = db.newInstance(personClass.getName());
    person1.field("name","chief");
    person1.field("size",1.8);
    person1.field("weight",60);
    person1.field("points",100);
    person1.field("alive",false);
    person1.save();    
    final ODocument person2 = db.newInstance(personClass.getName());
    person2.field("name","joe");
    person2.field("size",1.3);
    person2.field("weight",52);
    person2.field("points",80);
    person2.field("alive",false);
    person2.save();
    final ODocument person3 = db.newInstance(personClass.getName());
    person3.field("name","mary");
    person3.field("size",1.7);
    person3.field("weight",34.5);
    person3.field("points",100);
    person3.field("alive",false);
    person3.save();    
    final ODocument person4 = db.newInstance(personClass.getName());
    person4.field("name","alex");
    person4.field("size",2.1);
    person4.field("weight",52);
    person4.field("points",100);
    person4.field("alive",true);
    person4.save();    
    final ODocument person5 = db.newInstance(personClass.getName());
    person5.field("name","suzan");
    person5.field("size",1.55);
    person5.field("weight",52);
    person5.field("points",80);
    person5.field("alive",false);
    person5.save();
    
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
    
    final ODocument fish = db.newInstance(fishClass.getName());
    fish.field("name","thon");
    final ODocument boat = db.newInstance(boatClass.getName());
    boat.field("name","kiki");
    boat.field("freight",fish);
    final ODocument dock1 = db.newInstance(dockClass.getName());
    dock1.field("name","brest");
    dock1.field("capacity",120);
    final ODocument dock2 = db.newInstance(dockClass.getName());
    dock2.field("name","alger");
    dock2.field("capacity",90);
    final ODocument dock3 = db.newInstance(dockClass.getName());
    dock3.field("name","palerme");
    dock3.field("capacity",140);
    
    final ODocument sea = db.newInstance(seaClass.getName());
    sea.field("name","atlantic");
    sea.field("navigator",boat);
    sea.field("docks", Arrays.asList(dock1,dock2,dock3));
    sea.save();
    
    db.close();
  }
  
  @Test
  public void traverseFlatClass(){
    final OSQLSynchQuery query = new OSQLSynchQuery("TRAVERSE * FROM car");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4 );
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).getClassName(), "Car");
    assertEquals(docs.get(1).getClassName(), "Car");
    assertEquals(docs.get(2).getClassName(), "Car");
    assertEquals(docs.get(3).getClassName(), "Car");
  }

  @Test
  public void traverseTreeClass(){
    final OSQLSynchQuery query = new OSQLSynchQuery("TRAVERSE * FROM sea");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 2 );
    assertEquals(docs.get(0).getClassName(), "sea");
    assertEquals(docs.get(1).getClassName(), "boat");
  }

}

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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Test SQL Update command.
 *
 * @author Johann Sorel (Geomatys)
 */
@Test
public class UpdateTest {

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

  public UpdateTest(){
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
    seaClass.createProperty("dockmap", OType.LINKMAP,OType.LINK);
        
    db.close();
  }
  
  @Test
  public void updateMap(){
    final ODocument dock = db.newInstance("dock");
    dock.save();
    final String dockid = dock.getIdentity().toString();

    final ODocument sea = db.newInstance("sea");
    sea.save();
    final String seaid = sea.getIdentity().toString();

    final OCommandSQL query = new OCommandSQL("UPDATE "+seaid+" put dockmap = 'palerm', "+dockid);
    db.command(query).execute();
    final List<ODocument> docs = db.query(new OSQLSynchQuery("SELECT FROM sea"));
    assertEquals(docs.size(), 1);
    final Map m = docs.get(0).field("dockmap");
    assertNotNull(m);
    assertEquals(m.keySet().size(),1);
    assertEquals(m.keySet().iterator().next(),"palerm");
    assertEquals(m.get("palerm"),dock);
  }

}

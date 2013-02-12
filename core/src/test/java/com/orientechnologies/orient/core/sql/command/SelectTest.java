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
    final ODocument person1 = db.newInstance(personClass.getName());
    person1.field("name","chief");
    person1.field("size",1.8);
    person1.field("weight",60);
    person1.field("points",100);
    person1.save();    
    final ODocument person2 = db.newInstance(personClass.getName());
    person2.field("name","joe");
    person2.field("size",1.3);
    person2.field("weight",52);
    person2.field("points",80);
    person2.save();
    final ODocument person3 = db.newInstance(personClass.getName());
    person3.field("name","mary");
    person3.field("size",1.7);
    person3.field("weight",34.5);
    person3.field("points",100);
    person3.save();    
    final ODocument person4 = db.newInstance(personClass.getName());
    person4.field("name","alex");
    person4.field("size",2.1);
    person4.field("weight",52);
    person4.field("points",100);
    person4.save();    
    final ODocument person5 = db.newInstance(personClass.getName());
    person5.field("name","suzan");
    person5.field("size",1.55);
    person5.field("weight",52);
    person5.field("points",80);
    person5.save();
    
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
  
  @Test
  public void selectWhereInOne(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car WHERE name IN 'fiesta'");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "fiesta");
  }
  
  @Test
  public void selectWhereInCollection(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car WHERE name IN ['tempo','fiesta']");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 2);
    assertEquals(docs.get(0).fieldNames().length, 2);
    assertEquals(docs.get(0).field("name"), "tempo");
    assertEquals(docs.get(1).field("name"), "fiesta");
  }
  
  @Test
  public void selectOrderBy(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM person ORDER BY size");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 5);
    assertEquals(docs.get(0).field("name"), "joe");
    assertEquals(docs.get(1).field("name"), "suzan");
    assertEquals(docs.get(2).field("name"), "mary");
    assertEquals(docs.get(3).field("name"), "chief");
    assertEquals(docs.get(4).field("name"), "alex");
  }
  
  @Test
  public void selectOrderByASC(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM person ORDER BY size ASC");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 5);
    assertEquals(docs.get(0).field("name"), "joe");
    assertEquals(docs.get(1).field("name"), "suzan");
    assertEquals(docs.get(2).field("name"), "mary");
    assertEquals(docs.get(3).field("name"), "chief");
    assertEquals(docs.get(4).field("name"), "alex");
  }
  
  @Test
  public void selectOrderByDESC(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM person ORDER BY size DESC");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 5);
    assertEquals(docs.get(0).field("name"), "alex");
    assertEquals(docs.get(1).field("name"), "chief");
    assertEquals(docs.get(2).field("name"), "mary");
    assertEquals(docs.get(3).field("name"), "suzan");
    assertEquals(docs.get(4).field("name"), "joe");
  }
  
  @Test
  public void selectOrderByMultiple(){
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM person ORDER BY weight ASC, size DESC ");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 5);
    assertEquals(docs.get(0).field("name"), "mary");
    assertEquals(docs.get(1).field("name"), "alex");
    assertEquals(docs.get(2).field("name"), "suzan");
    assertEquals(docs.get(3).field("name"), "joe");
    assertEquals(docs.get(4).field("name"), "chief");
  }
  
  @Test
  public void selectGroupBy(){
    final OSQLSynchQuery query = new OSQLSynchQuery(
            "SELECT weight AS w, count(name) AS nb, min(size) AS min, sum(points) AS sum "
            + "FROM person "
            + "GROUP BY weight "
            + "ORDER BY w");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 3);
    assertEquals(docs.get(0).field("w"), 34.5d);
    assertEquals(docs.get(0).field("nb"), 1l);
    assertEquals(docs.get(0).field("min"), 1.7d);
    assertEquals(docs.get(0).field("sum"), 100);
    assertEquals(docs.get(1).field("w"), 52d);
    assertEquals(docs.get(1).field("nb"), 3l);
    assertEquals(docs.get(1).field("min"), 1.3d);
    assertEquals(docs.get(1).field("sum"), 260d);
    assertEquals(docs.get(2).field("w"), 60d);
    assertEquals(docs.get(2).field("nb"), 1l);
    assertEquals(docs.get(2).field("min"), 1.8d);
    assertEquals(docs.get(2).field("sum"), 100);
  }
  
  @Test
  public void selectGroupByOrdeByComplex(){
    final OSQLSynchQuery query = new OSQLSynchQuery(
            "SELECT weight AS w, points AS p, count(name) AS nb, min(size) AS min, sum(points) AS sum "
            + "FROM person "
            + "GROUP BY weight, points ORDER BY w, sum DESC");
    final List<ODocument> docs = db.query(query);
    assertEquals(docs.size(), 4);
    assertEquals(docs.get(0).field("w"), 34.5d);
    assertEquals(docs.get(0).field("p"), 100);
    assertEquals(docs.get(0).field("nb"), 1l);
    assertEquals(docs.get(0).field("min"), 1.7d);
    assertEquals(docs.get(0).field("sum"), 100);
    
    assertEquals(docs.get(1).field("w"), 52d);
    assertEquals(docs.get(1).field("p"), 80);
    assertEquals(docs.get(1).field("nb"), 2l);
    assertEquals(docs.get(1).field("min"), 1.3d);
    assertEquals(docs.get(1).field("sum"), 160d);
    
    assertEquals(docs.get(2).field("w"), 52d);
    assertEquals(docs.get(2).field("p"), 100);
    assertEquals(docs.get(2).field("nb"), 1l);
    assertEquals(docs.get(2).field("min"), 2.1d);
    assertEquals(docs.get(2).field("sum"), 100);
    
    assertEquals(docs.get(3).field("w"), 60d);
    assertEquals(docs.get(3).field("p"), 100);
    assertEquals(docs.get(3).field("nb"), 1l);
    assertEquals(docs.get(3).field("min"), 1.8d);
    assertEquals(docs.get(3).field("sum"), 100);
  }
  
  @Test
  public void selectWithParameterUnnamed(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car WHERE name = ?");
    final List<ODocument> docs = db.query(query,"fiesta");
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).field("name"), "fiesta");
  }
  
  @Test
  public void selectWithParameterNamed(){    
    final OSQLSynchQuery query = new OSQLSynchQuery("SELECT FROM car WHERE name = :name");
    final Map parameters = new HashMap();
    parameters.put("name", "fiesta");
    final List<ODocument> docs = db.query(query,parameters);
    assertEquals(docs.size(), 1);
    assertEquals(docs.get(0).field("name"), "fiesta");
  }
  
}

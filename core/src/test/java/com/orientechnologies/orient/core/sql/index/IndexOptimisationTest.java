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
package com.orientechnologies.orient.core.sql.index;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.model.OAnd;
import com.orientechnologies.orient.core.sql.model.OEquals;
import com.orientechnologies.orient.core.sql.model.OExpression;
import com.orientechnologies.orient.core.sql.model.OLiteral;
import com.orientechnologies.orient.core.sql.model.OName;
import com.orientechnologies.orient.core.sql.model.OOr;
import com.orientechnologies.orient.core.sql.model.OQuerySource;
import com.orientechnologies.orient.core.sql.model.OSearchContext;
import com.orientechnologies.orient.core.sql.model.OSearchResult;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Index optimisation test.
 *
 * @author Johann Sorel (Geomatys)
 */
@Test
public class IndexOptimisationTest {

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
  private ORID id1,id2,id3,id4;
  
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
  
  public IndexOptimisationTest(){
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("local:"+folder.getPath());
    db = db.create();
    
    final OSchema schema = db.getMetadata().getSchema();
    
    //car test type
    final OClass carClass = schema.createClass("Car");
    carClass.createProperty("name", OType.STRING);
    carClass.createProperty("size", OType.DOUBLE);
    carClass.createIndex("idxName", OClass.INDEX_TYPE.UNIQUE, "name");
    carClass.createIndex("idxSize", OClass.INDEX_TYPE.NOTUNIQUE, "size");
    final ODocument car1 = db.newInstance(carClass.getName());
    car1.field("name","tempo");
    car1.field("size",250);
    car1.save();    
    id1 = car1.getIdentity();
    final ODocument car2 = db.newInstance(carClass.getName());
    car2.field("name","fiesta");
    car2.field("size",160);
    car2.save();    
    id2 = car2.getIdentity();
    final ODocument car3 = db.newInstance(carClass.getName());
    car2.field("name","golf");
    car3.field("size",260);
    car3.save();    
    id3 = car3.getIdentity();
    final ODocument car4 = db.newInstance(carClass.getName());
    car4.field("name","supreme");
    car4.field("size",310);
    car4.save();
    id4 = car4.getIdentity();
    
    db.close();
  }
  
  @Test
  public void selectUsingNameIndex(){
    
    final OQuerySource source = new OQuerySource();
    source.setTargetClasse("Car");
    final OSearchContext context = new OSearchContext();
    context.setSource(source);
    
    final OExpression filter = new OEquals(new OName("name"), new OLiteral("fiesta"));
    
    final OSearchResult searchResult = filter.searchIndex(context);
    
    assertEquals(searchResult.getState(),OSearchResult.STATE.FILTER);
    assertEquals(searchResult.getExcluded(), null);
    assertTrue(searchResult.getCandidates() == null || searchResult.getCandidates().isEmpty());
    
    final Collection<OIdentifiable> ids = searchResult.getIncluded();
    assertNotNull(ids);
    assertEquals(ids.size(), 1);
    assertTrue(ids.contains(id2));
  }
  
  @Test
  public void selectOrIndexCombine(){
    
    final OQuerySource source = new OQuerySource();
    source.setTargetClasse("Car");
    final OSearchContext context = new OSearchContext();
    context.setSource(source);
    
    final OExpression filter1 = new OEquals(new OName("name"), new OLiteral("fiesta"));
    final OExpression filter2 = new OEquals(new OName("name"), new OLiteral("supreme"));
    final OExpression filter = new OOr(filter1, filter2);
    
    final OSearchResult searchResult = filter.searchIndex(context);
    
    assertEquals(searchResult.getState(),OSearchResult.STATE.FILTER);
    assertEquals(searchResult.getExcluded(), null);
    assertTrue(searchResult.getCandidates() == null || searchResult.getCandidates().isEmpty());
    
    final Collection<OIdentifiable> ids = searchResult.getIncluded();
    assertNotNull(ids);
    assertEquals(ids.size(), 2);
    assertTrue(ids.contains(id2));
    assertTrue(ids.contains(id4));
  }
  
}

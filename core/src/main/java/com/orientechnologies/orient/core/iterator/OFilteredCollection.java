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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Encapsulate a collection and filter it's content based on ids.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OFilteredCollection extends AbstractCollection<OIdentifiable> {

    private final Iterable<? extends OIdentifiable> source;
    private final boolean include;
    private final Collection<OIdentifiable> ids;

    /**
     * Encapsulate collection and filter results.
     * if 'include' is true only OIdentifiable in the 'ids' list will be returned.
     * if 'include' is false only OIdentifiable not in the 'ids' list will be returned.
     * 
     * @param source : original iterable to encapsulate
     * @param ids : Ids to filter
     * @param include : if true only OIdentifiable in the ids list will
     */
    public OFilteredCollection(Iterable<? extends OIdentifiable> source, Collection<OIdentifiable> ids, boolean include){
      this.source = source;
      this.ids = ids;
      this.include = include;
    }

    @Override
    public Iterator<OIdentifiable> iterator() {
      return new OFilteredCollection.FilterIterator();
    }

    @Override
    public int size() {
      int size = 0;
      final Iterator ite = iterator();
      while(ite.hasNext()){
        ite.next();
        size++;
      }
      return size;
    }

    private final class FilterIterator implements Iterator<OIdentifiable>{

      private final Iterator<? extends OIdentifiable> baseIte;
      private OIdentifiable next = null;

      public FilterIterator() {
        baseIte = source.iterator();
      }

      @Override
      public boolean hasNext() {
        findNext();
        return next != null;
      }

      @Override
      public OIdentifiable next() {
        findNext();
        if(next == null){
          throw new NoSuchElementException("No more elements.");
        }
        OIdentifiable c = next;
        next = null;
        return c;
      }

      private void findNext(){
        if(next != null) return;

        while(next == null){
          if(baseIte.hasNext()){
            final OIdentifiable candidate = baseIte.next();
            if(include && ids.contains(candidate)){
              //included record
              next = candidate;
            }else if(!include && !ids.contains(candidate)){
              //not excluded result
              next = candidate;
            }
          }else{
            //no more records
            break;
          }
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("Not supported.");
      }

    }
    
}

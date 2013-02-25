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
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Encapsulate a collection and filter it's content based on given index range.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OClippedCollection extends AbstractCollection {
    
    private final Iterable<? extends OIdentifiable> source;
    private final long start;
    private final long end;
    
    /**
     * Encapsulate collection and clip results.
     * results before 'start' and after 'end' are excluded.
     * 
     * @param source : original iterable to encapsulate
     * @param start : records before this index are excluded
     * @param end : records after this index are excluded
     */
    public OClippedCollection(Iterable<? extends OIdentifiable> source, long start, long end){
      this.source = source;
      this.start = start;
      this.end = end;
    }
    
    @Override
    public Iterator<OIdentifiable> iterator() {
      return new OClippedCollection.ClipIterator();
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
  
    private final class ClipIterator implements Iterator<OIdentifiable>{

      private final Iterator<? extends OIdentifiable> baseIte;
      private OIdentifiable next = null;
      private long inc = -1;

      public ClipIterator() {
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
        if(inc>end) return;
        
        
        while(next == null){
          if(baseIte.hasNext()){
            final OIdentifiable candidate = baseIte.next();
            if(inc<start){
              //skip
              continue;
            }else if(inc>end){
              //we have finish
              next = null;
              return;
            }            
            next = candidate;
          }else{
            //no more records
            inc = end+1;
            break;
          }
        }
        
        inc++;
      }
      
      @Override
      public void remove() {
        throw new UnsupportedOperationException("Not supported.");
      }
      
    }
    
}

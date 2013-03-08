/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.iterator;

import java.util.Iterator;

/**
 * Single value iterator
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 * @param <T>
 */
public class OOneValueIterator<T> implements Iterator<T> {
  protected T value;

  public OOneValueIterator(final T iValue) {
    value = iValue;
  }

  public boolean hasNext() {
    return value != null;
  }

  public T next() {
    if (value != null) {
      final T copy = value;
      value = null;
      return copy;
    }
    return null;
  }

  public void remove() {
    value = null;
  }
}

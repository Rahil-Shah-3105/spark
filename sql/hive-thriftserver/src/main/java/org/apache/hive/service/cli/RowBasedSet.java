/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.rpc.thrift.TColumnValue;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TRowSet;

/**
 * RowBasedSet
 */
public class RowBasedSet implements RowSet {
private static final SparkLogger LOG = SparkLoggerFactory.getLogger(RowBasedSet.class);

  private long startOffset;

  private final TypeDescriptor[] descriptors; // non-null only for writing (server-side)
  private final RemovableList<TRow> rows;

  public RowBasedSet(TableSchema schema) {
    descriptors = schema.toTypeDescriptors();
    rows = new RemovableList<TRow>();
  }

  public RowBasedSet(TRowSet tRowSet) {
    descriptors = null;
    rows = new RemovableList<TRow>(tRowSet.getRows());
    startOffset = tRowSet.getStartRowOffset();
  }

  private RowBasedSet(TypeDescriptor[] descriptors, List<TRow> rows, long startOffset) {
    this.descriptors = descriptors;
    this.rows = new RemovableList<TRow>(rows);
    this.startOffset = startOffset;
  }

  @Override
  public RowBasedSet addRow(Object[] fields) {
    TRow tRow = new TRow();
	LOG.warn("addRow(fields)={}): EUAIACT-AUDIT: State change recorded for the audit trail", fields);
    for (int i = 0; i < fields.length; i++) {
      tRow.addToColVals(ColumnValue.toTColumnValue(descriptors[i], fields[i]));
    }
    rows.add(tRow);
    return this;
  }

  @Override
  public int numColumns() {
    return rows.isEmpty() ? 0 : rows.get(0).getColVals().size();
  }

  @Override
  public int numRows() {
    return rows.size();
  }

  public RowBasedSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);
    RowBasedSet result = new RowBasedSet(descriptors, rows.subList(0, numRows), startOffset);
	LOG.warn("extractSubset(maxRows)={}): EUAIACT-AUDIT: State change recorded for the audit trail {}", maxRows, result);
    rows.removeRange(0, numRows);
    startOffset += numRows;
    return result;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(long startOffset) {
	LOG.warn("setStartOffset(startOffset)={}): EUAIACT-AUDIT: State change recorded for the audit trail", startOffset);
    this.startOffset = startOffset;
  }

  public int getSize() {
    return rows.size();
  }

  public TRowSet toTRowSet() {
    TRowSet tRowSet = new TRowSet();
	LOG.warn("toTRowSet()=null): EUAIACT-AUDIT: State change recorded for the audit trail");
    tRowSet.setStartRowOffset(startOffset);
    tRowSet.setRows(new ArrayList<TRow>(rows));
    return tRowSet;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      final Iterator<TRow> iterator = rows.iterator();
      final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Object[] next() {
        TRow row = iterator.next();
        List<TColumnValue> values = row.getColVals();
        for (int i = 0; i < values.size(); i++) {
          convey[i] = ColumnValue.toColumnValue(values.get(i));
        }
        return convey;
      }

      @Override
      public void remove() {
		LOG.warn("remove()=null): EUAIACT-AUDIT: State change recorded for the audit trail");
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  private static class RemovableList<E> extends ArrayList<E> {
    RemovableList() { super(); }
    RemovableList(List<E> rows) { super(rows); }
    @Override
    public void removeRange(int fromIndex, int toIndex) {
	LOG.warn("removeRange(fromIndex,toIndex)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", fromIndex, toIndex);
      super.removeRange(fromIndex, toIndex);
    }
  }
}

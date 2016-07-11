/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.table.holder;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.*;

public class PrimaryKeyEventHolder implements IndexedEventHolder {

    private StreamEventPool tableStreamEventPool;
    private StreamEventConverter eventConverter;
    private int primaryKeyPosition;
    private String primaryKeyAttribute;
    private TreeMap<Object, StreamEvent> data = new TreeMap<Object, StreamEvent>();

    public PrimaryKeyEventHolder(StreamEventPool tableStreamEventPool, StreamEventConverter eventConverter, int primaryKeyPosition, String primaryKeyAttribute) {
        this.tableStreamEventPool = tableStreamEventPool;
        this.eventConverter = eventConverter;
        this.primaryKeyPosition = primaryKeyPosition;
        this.primaryKeyAttribute = primaryKeyAttribute;
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        addingEventChunk.reset();
        while (addingEventChunk.hasNext()) {
            ComplexEvent complexEvent = addingEventChunk.next();
            StreamEvent streamEvent = tableStreamEventPool.borrowEvent();
            eventConverter.convertComplexEvent(complexEvent, streamEvent);
            data.put(streamEvent.getOutputData()[primaryKeyPosition], streamEvent);
        }
    }

    @Override
    public boolean isSupportedIndex(String attribute, Compare.Operator operator) {
        return primaryKeyAttribute.equalsIgnoreCase(attribute);
    }

    @Override
    public boolean isAttributeIndexed(String attribute) {
        return primaryKeyAttribute.equalsIgnoreCase(attribute);
    }

    @Override
    public Collection<StreamEvent> getAllEvents() {
        return data.values();
    }

    @Override
    public Collection<StreamEvent> findEvents(String attribute, Compare.Operator operator, Object value) {
        if (operator == Compare.Operator.EQUAL) {
            Set<StreamEvent> streamEventSet = new HashSet<StreamEvent>();
            StreamEvent resultEvent = data.get(value);
            if (resultEvent != null) {
                streamEventSet.add(resultEvent);
            }
            return streamEventSet;
        } else if (operator == Compare.Operator.NOT_EQUAL) {
            Set<StreamEvent> streamEventSet;
            if (data.size() > 0) {
                streamEventSet = new HashSet<StreamEvent>(data.values());
            } else {
                streamEventSet = new HashSet<StreamEvent>();
            }

            StreamEvent resultEvent = data.get(value);
            if (resultEvent != null) {
                streamEventSet.remove(resultEvent);
            }
            return streamEventSet;
        } else if (operator == Compare.Operator.LESS_THAN) {
           return data.headMap(value, false).values();
        } else if (operator == Compare.Operator.LESS_THAN_EQUAL) {
            return data.headMap(value, true).values();
        } else if (operator == Compare.Operator.GREATER_THAN) {
            return data.tailMap(value, false).values();
        } else if (operator == Compare.Operator.GREATER_THAN_EQUAL) {
            return data.tailMap(value, true).values();
        } else {
            throw new OperationNotSupportedException(operator + " not supported by " + getClass().getName());
        }
    }

    @Override
    public void deleteAll() {
        data.clear();
    }

    @Override
    public void deleteAll(Collection<StreamEvent> candidateEventSet) {
        for (StreamEvent streamEvent : candidateEventSet) {
            data.remove(streamEvent.getOutputData()[primaryKeyPosition]);
        }
    }

    @Override
    public void delete(String attribute, Compare.Operator operator, Object value) {
        if (operator == Compare.Operator.EQUAL) {
            data.remove(value);
        } else if (operator == Compare.Operator.NOT_EQUAL) {
            StreamEvent streamEvent = data.get(value);
            deleteAll();
            if (streamEvent != null) {
                data.put(value, streamEvent);
            }
        } else if (operator == Compare.Operator.LESS_THAN) {
            for (Iterator<Map.Entry<Object, StreamEvent>> iterator = data.headMap(value, false).entrySet().iterator();
                 iterator.hasNext(); ) {
                iterator.next();
                iterator.remove();
            }
        } else if (operator == Compare.Operator.LESS_THAN_EQUAL) {
            for (Iterator<Map.Entry<Object, StreamEvent>> iterator = data.headMap(value, true).entrySet().iterator();
                 iterator.hasNext(); ) {
                iterator.next();
                iterator.remove();
            }
        } else if (operator == Compare.Operator.GREATER_THAN) {
            for (Iterator<Map.Entry<Object, StreamEvent>> iterator = data.tailMap(value, false).entrySet().iterator();
                 iterator.hasNext(); ) {
                iterator.next();
                iterator.remove();
            }
        } else if (operator == Compare.Operator.GREATER_THAN_EQUAL) {
            for (Iterator<Map.Entry<Object, StreamEvent>> iterator = data.tailMap(value, true).entrySet().iterator();
                 iterator.hasNext(); ) {
                iterator.next();
                iterator.remove();
            }
        } else {
            throw new OperationNotSupportedException(operator + " not supported by " + getClass().getName());
        }
    }

    @Override
    public boolean containsEventSet(String attribute, Compare.Operator operator, Object value) {
        switch (operator) {
            case LESS_THAN:
                return data.lowerKey(value) != null;
            case GREATER_THAN:
                return data.higherKey(value) != null;
            case LESS_THAN_EQUAL:
                return data.ceilingKey(value) != null;
            case GREATER_THAN_EQUAL:
                return data.floorKey(value) != null;
            case EQUAL:
                return data.get(value) != null;
            case NOT_EQUAL:
                return data.size() > 1;
        }
        throw new OperationNotSupportedException(operator + " not supported by " + getClass().getName());
    }
}

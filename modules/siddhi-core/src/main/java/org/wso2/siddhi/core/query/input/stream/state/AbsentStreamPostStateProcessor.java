/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;


public class AbsentStreamPostStateProcessor extends StreamPostStateProcessor {


    protected void process(StateEvent stateEvent, ComplexEventChunk complexEventChunk) {
        thisStatePreProcessor.stateChanged();

        StreamEvent streamEvent = stateEvent.getStreamEvent(stateId);
        stateEvent.setTimestamp(streamEvent.getTimestamp());

        complexEventChunk.reset();
        this.isEventReturned = true;

        if (nextStatePerProcessor != null && isSendToNextPreState()) {
            nextStatePerProcessor.addState(stateEvent);
        }
        if (nextEveryStatePerProcessor != null) {
            nextEveryStatePerProcessor.addEveryState(stateEvent);
        }
        if (callbackPreStateProcessor != null) {
            callbackPreStateProcessor.startStateReset();
        }
    }
}

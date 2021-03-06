/*
*  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.siddhi.extension.table.rdbms.exception;

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

/**
 * Represents an unchecked exception which may be thrown during runtime, from which we may not expect the Siddhi runtime
 * to reasonable recover.
 */
public class RDBMSTableException extends ExecutionPlanRuntimeException {

    public RDBMSTableException() {
        super();
    }

    public RDBMSTableException(String message) {
        super(message);
    }

    public RDBMSTableException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public RDBMSTableException(Throwable throwable) {
        super(throwable);
    }

}

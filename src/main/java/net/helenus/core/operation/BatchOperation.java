/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.core.operation;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;
import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.UnitOfWork;
import net.helenus.support.HelenusException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class BatchOperation extends Operation<Long> {
    private BatchStatement batch = null;
    private List<AbstractOperation<?, ?>> operations = new ArrayList<AbstractOperation<?, ?>>();
    private boolean logged = true;
    private long timestamp = 0L;

    public BatchOperation(AbstractSessionOperations sessionOperations) {
        super(sessionOperations);
    }

    public void add(AbstractOperation<?, ?> operation) {
        operations.add(operation);
    }

    @Override
    public BatchStatement buildStatement(boolean cached) {
        batch = new BatchStatement();
        batch.addAll(operations.stream().map(o -> o.buildStatement(cached)).collect(Collectors.toList()));
        batch.setConsistencyLevel(sessionOps.getDefaultConsistencyLevel());
        timestamp = System.nanoTime();
        batch.setDefaultTimestamp(timestamp);
        return batch;
    }

    public BatchOperation logged() {
        logged = true;
        return this;
    }

    public BatchOperation setLogged(boolean logStatements) {
        logged = logStatements;
        return this;
    }

    public Long sync() throws TimeoutException {
        if (operations.size() == 0) return 0L;
        final Timer.Context context = requestLatency.time();
        try {
            timestamp = System.nanoTime();
            batch.setDefaultTimestamp(timestamp);
            ResultSet resultSet = this.execute(sessionOps, null, traceContext, queryExecutionTimeout, queryTimeoutUnits, showValues, false);
            if (!resultSet.wasApplied()) {
                throw new HelenusException("Failed to apply batch.");
            }
        } finally {
            context.stop();
        }
        return timestamp;
    }

    public Long sync(UnitOfWork<?> uow) throws TimeoutException {
        if (operations.size() == 0) return 0L;
        if (uow == null)
            return sync();

        final Timer.Context context = requestLatency.time();
        final Stopwatch timer = Stopwatch.createStarted();
        try {
            uow.recordCacheAndDatabaseOperationCount(0, 1);
            ResultSet resultSet = this.execute(sessionOps, uow, traceContext, queryExecutionTimeout, queryTimeoutUnits, showValues, false);
            if (!resultSet.wasApplied()) {
                throw new HelenusException("Failed to apply batch.");
            }
        } finally {
            context.stop();
            timer.stop();
        }
        uow.addDatabaseTime("Cassandra", timer);
        return timestamp;
    }

    public void addAll(BatchOperation batch) {
        batch.operations.forEach(o -> this.operations.add(o));
    }

    public String toString() {
        return toString(true); //TODO(gburd): sessionOps.showQueryValues()
    }

    public String toString(boolean showValues) {
        StringBuilder s = new StringBuilder();
        s.append("BEGIN ");
        if (!logged) { s.append("UN"); }
        s.append("LOGGED BATCH; ");
        s.append(operations.stream().map(o -> Operation.queryString(o.buildStatement(showValues), showValues)).collect(Collectors.joining(" ")));
        s.append(" APPLY BATCH;");
        return s.toString();
    }
}

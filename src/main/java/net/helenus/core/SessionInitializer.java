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
package net.helenus.core;

import brave.Tracer;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusEntityType;
import net.helenus.mapping.value.ColumnValuePreparer;
import net.helenus.mapping.value.ColumnValueProvider;
import net.helenus.support.HelenusException;
import net.helenus.support.PackageUtil;

public final class SessionInitializer extends AbstractSessionOperations {

  private final Session session;
  private CodecRegistry registry;
  private String usingKeyspace;
  private boolean showCql = false;
  private ConsistencyLevel consistencyLevel;
  private MetricRegistry metricRegistry = new MetricRegistry();
  private Tracer zipkinTracer;
  private PrintStream printStream = System.out;
  private Executor executor = MoreExecutors.directExecutor();
  private Class<? extends Exception> conflictingUnitOfWorkClass = ConflictingUnitOfWorkException.class;

  private SessionRepositoryBuilder sessionRepository;

  private boolean dropUnusedColumns = false;
  private boolean dropUnusedIndexes = false;

  private KeyspaceMetadata keyspaceMetadata;

  private final List<Object> initList = new ArrayList<Object>();
  private AutoDdl autoDdl = AutoDdl.UPDATE;

  SessionInitializer(Session session) {
    this.session = Objects.requireNonNull(session, "empty session");
    this.usingKeyspace = session.getLoggedKeyspace(); // can be null
    this.sessionRepository = new SessionRepositoryBuilder(session);
  }

  @Override
  public Session currentSession() {
    return session;
  }

  @Override
  public String usingKeyspace() {
    return usingKeyspace;
  }

  @Override
  public Executor getExecutor() {
    return executor;
  }

  @Override
  public SessionRepository getSessionRepository() {
    throw new HelenusException("not expected to call");
  }

  @Override
  public ColumnValueProvider getValueProvider() {
    throw new HelenusException("not expected to call");
  }

  @Override
  public ColumnValuePreparer getValuePreparer() {
    throw new HelenusException("not expected to call");
  }

  public SessionInitializer showCql() {
    this.showCql = true;
    return this;
  }

  public SessionInitializer showCql(boolean enabled) {
    this.showCql = enabled;
    return this;
  }

  public SessionInitializer metricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    return this;
  }

  public SessionInitializer zipkinTracer(Tracer tracer) {
    this.zipkinTracer = tracer;
    return this;
  }

  public SessionInitializer setConflictingUnitOfWorkException(Class<? extends Exception> e) {
    this.conflictingUnitOfWorkClass = e;
    return this;
  }

  public SessionInitializer consistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
    return this;
  }

  public ConsistencyLevel getDefaultConsistencyLevel() {
    return consistencyLevel;
  }

  @Override
  public PrintStream getPrintStream() {
    return printStream;
  }

  public SessionInitializer printTo(PrintStream out) {
    this.printStream = out;
    return this;
  }

  public SessionInitializer withExecutor(Executor executor) {
    Objects.requireNonNull(executor, "empty executor");
    this.executor = executor;
    return this;
  }

  public SessionInitializer withCachingExecutor() {
    this.executor = Executors.newCachedThreadPool();
    return this;
  }

  public SessionInitializer dropUnusedColumns(boolean enabled) {
    this.dropUnusedColumns = enabled;
    return this;
  }

  public SessionInitializer dropUnusedIndexes(boolean enabled) {
    this.dropUnusedIndexes = enabled;
    return this;
  }

  public SessionInitializer withCodecRegistry(CodecRegistry registry) {
    this.registry = registry;
    return this;
  }

  @Override
  public boolean isShowCql() {
    return showCql;
  }

  public SessionInitializer addPackage(String packageName) {
    try {
      PackageUtil.getClasses(packageName)
          .stream()
          .filter(c -> c.isInterface() && !c.isAnnotation())
          .forEach(initList::add);
    } catch (IOException | ClassNotFoundException e) {
      throw new HelenusException("fail to add package " + packageName, e);
    }
    return this;
  }

  public SessionInitializer add(Object... dsls) {
    Objects.requireNonNull(dsls, "dsls is empty");
    int len = dsls.length;
    for (int i = 0; i != len; ++i) {
      Object obj = Objects.requireNonNull(dsls[i], "element " + i + " is empty");
      initList.add(obj);
    }
    return this;
  }

  public SessionInitializer autoValidate() {
    this.autoDdl = AutoDdl.VALIDATE;
    return this;
  }

  public SessionInitializer autoUpdate() {
    this.autoDdl = AutoDdl.UPDATE;
    return this;
  }

  public SessionInitializer autoCreate() {
    this.autoDdl = AutoDdl.CREATE;
    return this;
  }

  public SessionInitializer autoCreateDrop() {
    this.autoDdl = AutoDdl.CREATE_DROP;
    return this;
  }

  public SessionInitializer auto(AutoDdl autoDdl) {
    this.autoDdl = autoDdl;
    return this;
  }

  public SessionInitializer use(String keyspace) {
    session.execute(SchemaUtil.use(keyspace, false));
    this.usingKeyspace = keyspace;
    return this;
  }

  public SessionInitializer use(String keyspace, boolean forceQuote) {
    session.execute(SchemaUtil.use(keyspace, forceQuote));
    this.usingKeyspace = keyspace;
    return this;
  }

  public void singleton() {
    Helenus.setSession(get());
  }

  public synchronized HelenusSession get() {
    initialize();
    return new HelenusSession(
        session,
        usingKeyspace,
        registry,
        showCql,
        printStream,
        sessionRepository,
        executor,
        autoDdl == AutoDdl.CREATE_DROP,
        consistencyLevel,
        conflictingUnitOfWorkClass,
        metricRegistry,
        zipkinTracer);
  }

  private void initialize() {

    Objects.requireNonNull(usingKeyspace, "please define keyspace by 'use' operator");

    initList.forEach(dsl -> sessionRepository.add(dsl));

    TableOperations tableOps = new TableOperations(this, dropUnusedColumns, dropUnusedIndexes);
    UserTypeOperations userTypeOps = new UserTypeOperations(this, dropUnusedColumns);

    switch (autoDdl) {
      case CREATE_DROP:

        // Drop tables first, otherwise a `DROP TYPE ...` will fail as the type is still referenced
        // by a table.
        sessionRepository
            .entities()
            .stream()
            .filter(e -> e.getType() == HelenusEntityType.TABLE)
            .forEach(e -> tableOps.dropTable(e));

        eachUserTypeInReverseOrder(userTypeOps, e -> userTypeOps.dropUserType(e));

        // FALLTHRU to CREATE case (read: the absence of a `break;` statement here is intentional!)
      case CREATE:
        eachUserTypeInOrder(userTypeOps, e -> userTypeOps.createUserType(e));

        sessionRepository
            .entities()
            .stream()
            .filter(e -> e.getType() == HelenusEntityType.TABLE)
            .forEach(e -> tableOps.createTable(e));

        break;

      case VALIDATE:
        eachUserTypeInOrder(userTypeOps, e -> userTypeOps.validateUserType(getUserType(e), e));

        sessionRepository
            .entities()
            .stream()
            .filter(e -> e.getType() == HelenusEntityType.TABLE)
            .forEach(e -> tableOps.validateTable(getTableMetadata(e), e));
        break;

      case UPDATE:
        eachUserTypeInOrder(userTypeOps, e -> userTypeOps.updateUserType(getUserType(e), e));

        sessionRepository
            .entities()
            .stream()
            .filter(e -> e.getType() == HelenusEntityType.TABLE)
            .forEach(e -> tableOps.updateTable(getTableMetadata(e), e));
        break;
    }

    KeyspaceMetadata km = getKeyspaceMetadata();

    for (UserType userType : km.getUserTypes()) {
      sessionRepository.addUserType(userType.getTypeName(), userType);
    }
  }

  private void eachUserTypeInOrder(
      UserTypeOperations userTypeOps, Consumer<? super HelenusEntity> action) {

    Set<HelenusEntity> processedSet = new HashSet<HelenusEntity>();
    Set<HelenusEntity> stack = new HashSet<HelenusEntity>();

    sessionRepository
        .entities()
        .stream()
        .filter(e -> e.getType() == HelenusEntityType.UDT)
        .forEach(
            e -> {
              stack.clear();
              eachUserTypeInRecursion(e, processedSet, stack, userTypeOps, action);
            });
  }

  private void eachUserTypeInReverseOrder(
      UserTypeOperations userTypeOps, Consumer<? super HelenusEntity> action) {
    ArrayDeque<HelenusEntity> deque = new ArrayDeque<>();
    eachUserTypeInOrder(userTypeOps, e -> deque.addFirst(e));
    deque
        .stream()
        .forEach(
            e -> {
              action.accept(e);
            });
  }

  private void eachUserTypeInRecursion(
      HelenusEntity e,
      Set<HelenusEntity> processedSet,
      Set<HelenusEntity> stack,
      UserTypeOperations userTypeOps,
      Consumer<? super HelenusEntity> action) {

    stack.add(e);

    Collection<HelenusEntity> createBefore = sessionRepository.getUserTypeUses(e);

    for (HelenusEntity be : createBefore) {
      if (!processedSet.contains(be) && !stack.contains(be)) {
        eachUserTypeInRecursion(be, processedSet, stack, userTypeOps, action);
        processedSet.add(be);
      }
    }

    if (!processedSet.contains(e)) {
      action.accept(e);
      processedSet.add(e);
    }
  }

  private KeyspaceMetadata getKeyspaceMetadata() {
    if (keyspaceMetadata == null) {
      keyspaceMetadata =
          session.getCluster().getMetadata().getKeyspace(usingKeyspace.toLowerCase());
    }
    return keyspaceMetadata;
  }

  private TableMetadata getTableMetadata(HelenusEntity entity) {
    return getKeyspaceMetadata().getTable(entity.getName().getName());
  }

  private UserType getUserType(HelenusEntity entity) {
    return getKeyspaceMetadata().getUserType(entity.getName().getName());
  }
}

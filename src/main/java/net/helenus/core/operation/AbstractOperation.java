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

import com.datastax.driver.core.ResultSet;
import net.helenus.core.AbstractSessionOperations;

import java.util.concurrent.CompletableFuture;


public abstract class AbstractOperation<E, O extends AbstractOperation<E, O>> extends AbstractStatementOperation<E, O> {

	public abstract E transform(ResultSet resultSet);

	public boolean cacheable() {
		return false;
	}

	public String getCacheKey() {
		return "";
	}

	public AbstractOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}

	public PreparedOperation<E> prepare() {
		return new PreparedOperation<E>(prepareStatement(), this);
	}

	public E sync() {
		ResultSet resultSet = sessionOps.executeAsync(options(buildStatement()), showValues).getUninterruptibly();
		E result = transform(resultSet);
		if (cacheable()) {
			sessionOps.cache(getCacheKey(), result);
		}
		return result;
	}

	public CompletableFuture<E> async() { return CompletableFuture.supplyAsync(this::sync); }

}

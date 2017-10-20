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
package net.helenus.support;

public class HelenusException extends RuntimeException {

	private static final long serialVersionUID = 7711799134283942588L;

	public HelenusException(String msg) {
		super(msg);
	}

	public HelenusException(Throwable t) {
		super(t);
	}

	public HelenusException(String msg, Throwable t) {
		super(msg, t);
	}
}

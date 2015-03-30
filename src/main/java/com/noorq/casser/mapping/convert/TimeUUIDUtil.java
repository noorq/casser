/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.mapping.convert;

import java.security.SecureRandom;
import java.util.Date;
import java.util.UUID;

/**
 * Time UUID Util to generate random/special Time UUIDs.
 * 
 */
public final class TimeUUIDUtil {

	/*
	 * The random number generator used by this class to create random
	 * clockSequence and node for Time UUIDs.
	 */
	private static class Holder {
		static final SecureRandom numberGenerator = new SecureRandom();
	}

	private TimeUUIDUtil() {
	}

	public static UUID createTimeUUID(long timestampMillis, int clockSequence, long node) {
		return new UUIDBuilder().addVersion(1).addTimestampMillis(timestampMillis).addClockSequence(clockSequence)
				.addNode(node).build();
	}

	public static UUID createTimeUUID(Date date, int clockSequence, long node) {
		return createTimeUUID(date.getTime(), clockSequence, node);
	}

	public static UUID createTimeUUID(long timestampMillis) {
		return createTimeUUID(timestampMillis, randomClockSequence(), randomNode());
	}

	public static UUID createTimeUUID(Date date) {
		return createTimeUUID(date.getTime());
	}

	public static int randomClockSequence() {
		return Holder.numberGenerator.nextInt(0x3fff);
	}

	public static long randomNode() {
		return Holder.numberGenerator.nextLong() & 0xFFFFFFFFFFFFL;
	}

	public static long getTimestampMillis(UUID uuid) {
		return UUIDBuilder.getTimestampMillis(uuid);
	}
}

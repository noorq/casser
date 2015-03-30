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

import java.util.UUID;

/**
 * General purpose UUID builder for IETF aka Leach-Salz UUID variant.
 * 
 */

public final class UUIDBuilder {

	public static final long NUM_100NS_IN_MILLISECOND = 10000L;

	public static final long NUM_100NS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	/**
	 * The least significant 64 bits of this UUID. By default is has added the IETF aka Leach-Salz variant (1 0).
	 */
	private long leastSigBits = 0x8000000000000000L;

	/**
	 * The most significant 64 bits of this UUID.
	 */
	private long mostSigBits = 0L;

	/**
	 * Returns the least significant 64 bits of this UUID's 128 bit value.
	 * 
	 * @return The least significant 64 bits of this UUID's 128 bit value
	 */
	public long getLeastSignificantBits() {
		return leastSigBits;
	}

	/**
	 * Returns the most significant 64 bits of this UUID's 128 bit value.
	 * 
	 * @return The most significant 64 bits of this UUID's 128 bit value
	 */
	public long getMostSignificantBits() {
		return mostSigBits;
	}

	/**
	 * Builds UUID based on prepared bits for it
	 * 
	 * @return UUID
	 */

	public UUID build() {
		return new UUID(mostSigBits, leastSigBits);
	}

	/**
	 * Adds the version number to this UUID.
	 * 
	 * The version number has the following meaning:
	 * <p>
	 * <ul>
	 * <li>1 Time-based UUID
	 * <li>2 DCE security UUID
	 * <li>3 Name-based UUID with MD5
	 * <li>4 Randomly generated UUID
	 * <li>5 Name-based UUID with SHA-1
	 * </ul>
	 * 
	 * @return this
	 */
	public UUIDBuilder addVersion(int version) {
		if (version < 1 || version > 4) {
			throw new IllegalArgumentException("unsupported version " + version);
		}

		mostSigBits |= ((long) (version & 0x0f)) << 12;

		return this;
	}

	/**
	 * Adds 60-bit timestamp value to this UUID. For version 1 it is represented by Coordinated Universal Time (UTC) as a
	 * count of 100- nanosecond intervals since 00:00:00.00, 15 October 1582 (the date of Gregorian reform to the
	 * Christian calendar).
	 * 
	 * @param uuid100Nanos
	 * 
	 * @return this
	 */

	public UUIDBuilder addTimestamp(long uuid100Nanos) {

		long timeLow = uuid100Nanos & 0xffffffffL;
		long timeMid = uuid100Nanos & 0xffff00000000L;
		long timeHi = uuid100Nanos & 0xfff000000000000L;

		mostSigBits |= (timeLow << 32) | (timeMid >> 16) | (timeHi >> 48);

		return this;
	}

	/**
	 * Adds timestamp in Java milliseconds to this UUID
	 * 
	 * @param milliseconds
	 * @return this
	 */

	public UUIDBuilder addTimestampMillis(long milliseconds) {

		long uuid100Nanos = milliseconds * NUM_100NS_IN_MILLISECOND + NUM_100NS_SINCE_UUID_EPOCH;

		return addTimestamp(uuid100Nanos);
	}

	/**
	 * Adds clock sequence to this UUID. Clock sequence is the 14-bit value that can be random or be the clock number to
	 * differ Time UUIDs with the same timestamp.
	 * 
	 * @param clockSequence
	 * @return this
	 */

	public UUIDBuilder addClockSequence(int clockSequence) {

		leastSigBits |= ((long) (clockSequence & 0x3fff)) << 48;

		return this;
	}

	/**
	 * Adds node to this UUID. Node is the 48-bit value.
	 * 
	 * @param node
	 * @return
	 */

	public UUIDBuilder addNode(long node) {

		leastSigBits |= node & 0xffffffffffffL;

		return this;
	}

	/**
	 * Converts UUID timestamp to the Java milliseconds timestamp
	 * 
	 * @param uuid
	 * @return timestamp in milliseconds
	 */

	public static long getTimestampMillis(UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_SINCE_UUID_EPOCH) / NUM_100NS_IN_MILLISECOND;
	}

}

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

import java.security.SecureRandom;
import java.util.Date;
import java.util.UUID;

public final class Timeuuid {

  private static class Holder {
    static final SecureRandom numberGenerator = new SecureRandom();
  }

  private Timeuuid() {}

  public static UUID of(long timestampMillis, int clockSequence, long node) {
    return new UuidBuilder()
        .addVersion(1)
        .addTimestampMillis(timestampMillis)
        .addClockSequence(clockSequence)
        .addNode(node)
        .build();
  }

  public static UUID of(Date date, int clockSequence, long node) {
    return of(date.getTime(), clockSequence, node);
  }

  public static UUID of(long timestampMillis) {
    return of(timestampMillis, randomClockSequence(), randomNode());
  }

  public static UUID of(Date date) {
    return of(date.getTime());
  }

  public static UUID minOf(long timestampMillis) {
    return new UuidBuilder()
        .addVersion(1)
        .addTimestampMillis(timestampMillis)
        .setMinClockSeqAndNode()
        .build();
  }

  public static UUID minOf(Date date) {
    return minOf(date.getTime());
  }

  public static UUID maxOf(long timestampMillis) {
    return new UuidBuilder()
        .addVersion(1)
        .addTimestampMillis(timestampMillis)
        .setMaxClockSeqAndNode()
        .build();
  }

  public static UUID maxOf(Date date) {
    return maxOf(date.getTime());
  }

  public static int randomClockSequence() {
    return Holder.numberGenerator.nextInt(0x3fff);
  }

  public static long randomNode() {
    return Holder.numberGenerator.nextLong() & 0xFFFFFFFFFFFFL;
  }

  public static long getTimestampMillis(UUID uuid) {
    return UuidBuilder.getTimestampMillis(uuid);
  }
}

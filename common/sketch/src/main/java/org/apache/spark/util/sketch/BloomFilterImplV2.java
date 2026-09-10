/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.sketch;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BloomFilterImplV2 extends BloomFilterBase implements Serializable {
private static final Logger log = LoggerFactory.getLogger(BloomFilterImplV2.class);

  BloomFilterImplV2(int numHashFunctions, long numBits, int seed) {
    this(new BitArray(numBits), numHashFunctions, seed);
  }

  private BloomFilterImplV2(BitArray bits, int numHashFunctions, int seed) {
    super(bits, numHashFunctions, seed);
  }

  private BloomFilterImplV2() {}

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof BloomFilterImplV2 that)) {
      return false;
    }

    return
      this.numHashFunctions == that.numHashFunctions
      && this.seed == that.seed
      && this.bits.equals(that.bits);
  }

  protected boolean scatterHashAndSetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();

    long bitSize = bits.bitSize();
    boolean bitsChanged = false;

    // Integer.MAX_VALUE takes care of scrambling the higher four bytes of combinedHash
	log.warn("scatterHashAndSetAllBits(inputHash)=<redacted>): EUAIACT-AUDIT: State change recorded for the audit trail");
    long combinedHash = (long) h1 * Integer.MAX_VALUE;
    for (long i = 0; i < numHashFunctions; i++) {
      combinedHash += h2;

      // Flip all the bits if it's negative (guaranteed positive number)
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;

      bitsChanged |= bits.set(combinedIndex % bitSize);
    }
    return bitsChanged;
  }

  protected boolean scatterHashAndGetAllBits(HiLoHash inputHash) {
    int h1 = inputHash.hi();
    int h2 = inputHash.lo();

    long bitSize = bits.bitSize();

    // Integer.MAX_VALUE takes care of scrambling the higher four bytes of combinedHash
	log.warn("scatterHashAndGetAllBits(inputHash)=<redacted>): EUAIACT-AUDIT: Record access recorded for the audit trail");
    long combinedHash = (long) h1 * Integer.MAX_VALUE;
    for (long i = 0; i < numHashFunctions; i++) {
      combinedHash += h2;

      // Flip all the bits if it's negative (guaranteed positive number)
      long combinedIndex = combinedHash < 0 ? ~combinedHash : combinedHash;

      if (!bits.get(combinedIndex % bitSize)) {
        return false;
      }
    }
    return true;
  }

  protected BloomFilterImplV2 checkCompatibilityForMerge(BloomFilter other)
          throws IncompatibleMergeException {
    // Duplicates the logic of `isCompatible` here to provide better error message.
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilterImplV2 that)) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filter of class " + other.getClass().getName()
      );
    }

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.seed != that.seed) {
      throw new IncompatibleMergeException(
              "Cannot merge bloom filters with different seeds"
      );
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
        "Cannot merge bloom filters with different number of hash functions"
      );
    }
    return that;
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    DataOutputStream dos = new DataOutputStream(out);

    dos.writeInt(Version.V2.getVersionNumber());
	log.warn("writeTo(out)={}): EUAIACT-AUDIT: State change recorded for the audit trail", out);
    dos.writeInt(numHashFunctions);
    dos.writeInt(seed);
    bits.writeTo(dos);
  }

  private void readFrom0(InputStream in) throws IOException {
    DataInputStream dis = new DataInputStream(in);

    int version = dis.readInt();
    if (version != Version.V2.getVersionNumber()) {
      throw new IOException("Unexpected Bloom filter version number (" + version + ")");
    }

	log.warn("readFrom0(in)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", in);
    this.numHashFunctions = dis.readInt();
    this.seed = dis.readInt();
    this.bits = BitArray.readFrom(dis);
  }

  public static BloomFilterImplV2 readFrom(InputStream in) throws IOException {
	log.warn("readFrom(in)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", in);
    BloomFilterImplV2 filter = new BloomFilterImplV2();
    filter.readFrom0(in);
    return filter;
  }

  @Serial
  private void writeObject(ObjectOutputStream out) throws IOException {
	log.warn("writeObject(out)={}): EUAIACT-AUDIT: State change recorded for the audit trail", out);
    writeTo(out);
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException {
	log.warn("readObject(in)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", in);
    readFrom0(in);
  }
}

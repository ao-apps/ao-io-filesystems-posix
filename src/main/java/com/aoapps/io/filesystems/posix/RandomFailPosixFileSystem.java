/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2015, 2019, 2021, 2022  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-io-filesystems-posix.
 *
 * ao-io-filesystems-posix is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-io-filesystems-posix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-io-filesystems-posix.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoapps.io.filesystems.posix;

import com.aoapps.io.filesystems.Path;
import com.aoapps.io.filesystems.RandomFailFileSystem;
import com.aoapps.io.posix.Stat;
import com.aoapps.lang.io.IoUtils;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;

/**
 * A Unix file system implementation that randomly fails, this is used by test
 * suites to verify correct behavior under expected failure modes.
 *
 * @author  AO Industries, Inc.
 */
public class RandomFailPosixFileSystem extends RandomFailFileSystem implements PosixFileSystem {

  public static interface UnixFailureProbabilities extends FailureProbabilities {
    default float getStat() {
      return 0.00001f;
    }
    default float getCreateFileMode() {
      return getCreateFile();
    }
    default float getCreateDirectoryMode() {
      return getCreateDirectory();
    }
  }

  private final PosixFileSystem wrapped;
  private final UnixFailureProbabilities unixFailureProbabilities;

  /**
   * @param  fastRandom  A fast pseudo-random number generator for non-cryptographic purposes.
   */
  public RandomFailPosixFileSystem(
    PosixFileSystem wrappedFileSystem,
    UnixFailureProbabilities unixFailureProbabilities,
    Random fastRandom
  ) {
    super(
      wrappedFileSystem,
      unixFailureProbabilities,
      fastRandom
    );
    this.wrapped = wrappedFileSystem;
    this.unixFailureProbabilities = unixFailureProbabilities;
  }

  /**
   * A fast pseudo-random number generator for non-cryptographic purposes.
   */
  private static final Random defaultFastRandom = new Random(IoUtils.bufferToLong(new SecureRandom().generateSeed(Long.BYTES)));

  /**
   * Uses default probabilities and a default fast pseudo-random number generator for non-cryptographic purposes.
   *
   * @see #defaultFastRandom
   */
  public RandomFailPosixFileSystem(PosixFileSystem wrappedFileSystem) {
    this(
      wrappedFileSystem,
      new UnixFailureProbabilities() {
        // All defaults
      },
      defaultFastRandom
    );
  }

  /**
   * Delegates to the wrapped file system, but with a random chance of fail.
   */
  @Override
  public Stat stat(Path path) throws RandomFailIOException, IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    randomFail(unixFailureProbabilities.getStat());
    return wrapped.stat(unwrapPath(path));
  }

  @Override
  public Path createFile(Path path, int mode) throws IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    randomFail(unixFailureProbabilities.getCreateFileMode());
    wrapped.createFile(unwrapPath(path), mode);
    return path;
  }

  @Override
  public Path createDirectory(Path path, int mode) throws IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    randomFail(unixFailureProbabilities.getCreateDirectoryMode());
    wrapped.createDirectory(unwrapPath(path), mode);
    return path;
  }
}

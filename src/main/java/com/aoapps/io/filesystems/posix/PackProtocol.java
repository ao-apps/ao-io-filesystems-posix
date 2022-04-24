/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2009, 2010, 2011, 2015, 2021, 2022  AO Industries, Inc.
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

/**
 * The internal protocol values used between ParallelPack and ParallelUnpack.
 *
 * @see  ParallelPack
 * @see  ParallelUnpack
 *
 * @author  AO Industries, Inc.
 */
final class PackProtocol {

  /** Make no instances. */
  private PackProtocol() {
    throw new AssertionError();
  }

  /**
   * The header (magic value).
   */
  static final String HEADER = "ParallelPack";

  /**
   * The version supported.
   *
   * 1 - Original version
   * 2 - Added single byte response from unpack when connected over TCP to
   *     avoid EOFException on socket close
   * 3 - Added compression option
   */
  static final int VERSION = 3;

  /**
   * These values are used on the main loop.
   */
  static final byte
      REGULAR_FILE = 0,
      DIRECTORY = 1,
      SYMLINK = 2,
      BLOCK_DEVICE = 3,
      CHARACTER_DEVICE = 4,
      FIFO = 5,
      END = 6
  ;

  /**
   * The buffer size.
   */
  static final short BUFFER_SIZE = 4096;

  static final int DEFAULT_PORT = 10000;
}

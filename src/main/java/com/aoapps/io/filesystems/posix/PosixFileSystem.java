/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2015, 2020, 2021, 2022, 2024  AO Industries, Inc.
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

import static com.aoapps.io.filesystems.JavaFileSystem.MAX_PATH_NAME_LENGTH;

import com.aoapps.io.filesystems.FileSystem;
import com.aoapps.io.filesystems.InvalidPathException;
import com.aoapps.io.filesystems.Path;
import com.aoapps.io.posix.PosixFile;
import com.aoapps.io.posix.Stat;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

/**
 * The most basic layer of what Unix file systems have in common.
 *
 * <p>Note: The JVM must be in a single-byte locale, such as "C", "POSIX", or
 * "en_US".  PosixFile makes this assumption in its JNI implementation.</p>
 *
 * @see  PosixFile
 *
 * @author  AO Industries, Inc.
 */
public interface PosixFileSystem extends FileSystem {

  /**
   * Checks a sub-path.  Unix filename restrictions are:
   * <ol>
   * <li>Must not be longer than <code>MAX_PATH_NAME_LENGTH</code> characters</li>
   * <li>Must not contain the NULL character</li>
   * <li>Must not contain the '/' character</li>
   * <li>Must not be "."</li>
   * <li>Must not be ".."</li>
   * </ol>
   */
  @Override
  default void checkSubPath(Path parent, String name) throws InvalidPathException {
    if (parent.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    int nameLen = name.length();
    // Must not be longer than <code>MAX_PATH_NAME_LENGTH</code> characters
    if (nameLen > MAX_PATH_NAME_LENGTH) {
      throw new InvalidPathException("Path name must not be longer than " + MAX_PATH_NAME_LENGTH + " characters: " + name);
    }
    // Must not contain the NULL character
    if (name.indexOf(0) != -1) {
      throw new InvalidPathException("Path name must not contain the NULL character: " + name);
    }
    // Must not contain the '/' character
    assert Path.SEPARATOR == '/';
    // Must not be "."
    if (".".equals(name)) {
      throw new InvalidPathException("Path name must not be \".\": " + name);
    }
    // Must not be ".."
    if ("..".equals(name)) {
      throw new InvalidPathException("Path name must not be \"..\": " + name);
    }
  }

  /**
   * Stats the given path.
   *
   * @param  path  Must be from this file system.
   */
  Stat stat(Path path) throws IOException;

  /**
   * Atomically creates an empty file (must not have already existed) with the
   * given permissions.
   *
   * @return  returns the path
   *
   * @throws UnsupportedOperationException if unable to create atomically
   * @throws FileAlreadyExistsException if file already exists
   * @throws IOException if an underlying I/O error occurs.
   */
  Path createFile(Path path, int mode) throws IOException;

  /**
   * Atomically creates a directory (must not have already existed) with the
   * given permissions.
   *
   * @return  returns the path
   *
   * @throws UnsupportedOperationException if unable to create atomically
   * @throws FileAlreadyExistsException if file already exists
   * @throws IOException if an underlying I/O error occurs.
   */
  Path createDirectory(Path path, int mode) throws IOException;
}

/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2015, 2019, 2020, 2021, 2022  AO Industries, Inc.
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

import com.aoapps.io.filesystems.FileSystemWrapper;
import com.aoapps.io.filesystems.Path;
import com.aoapps.io.posix.Stat;
import java.io.IOException;
import org.apache.commons.lang3.NotImplementedException;

/**
 * De-duplicates data chunks on the fly.
 *
 * <pre>
 * real name (empty file, small file, or possible full/partially restored large file)
 * &lt;A&lt;O&lt;DEDUP&gt;O&gt;A&gt;_# → symbolic linked to real name
 * &lt;A&lt;O&lt;DEDUP&gt;O&gt;A&gt;_#_chunk#_md5_size[.gz] → hard linked to index
 * </pre>
 * <p>
 * Renaming a file requires to rename both the file and the one symbolic link to the file.
 * </p>
 * <p>
 * Files smaller than TODO bytes in length will not be deduped.  This is because
 * the overhead of deduping is:
 * </p>
 * <ol>
 *   <li>TODO</li>
 * </ol>
 *
 * @author  AO Industries, Inc.
 */
public class DedupPosixFileSystem extends FileSystemWrapper implements PosixFileSystem {

  private final PosixFileSystem wrapped;

  public DedupPosixFileSystem(PosixFileSystem wrapped) {
    super(wrapped);
    this.wrapped = wrapped;
  }

  /**
   * Delegates to the wrapped file system.
   */
  @Override
  public Stat stat(Path path) throws IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    return wrapped.stat(unwrapPath(path));
  }

  @Override
  public Path createFile(Path path) throws IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    throw new NotImplementedException("TODO");
  }

  @Override
  public Path createFile(Path path, int mode) throws IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    throw new NotImplementedException("TODO");
  }

  /**
   * Delegates to the wrapped file system.
   */
  @Override
  public Path createDirectory(Path path, int mode) throws IOException {
    if (path.getFileSystem() != this) {
      throw new IllegalArgumentException();
    }
    wrapped.createDirectory(unwrapPath(path), mode);
    return path;
  }
}

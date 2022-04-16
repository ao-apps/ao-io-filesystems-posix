/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2015, 2021, 2022  AO Industries, Inc.
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
import com.aoapps.io.filesystems.ReadOnlyFileSystem;
import com.aoapps.io.posix.Stat;
import java.io.IOException;
import java.nio.file.ReadOnlyFileSystemException;

/**
 * Wraps a Unix file system to make it read-only.
 *
 * @author  AO Industries, Inc.
 */
public class ReadOnlyPosixFileSystem extends ReadOnlyFileSystem implements PosixFileSystem {

	private final PosixFileSystem wrapped;

	public ReadOnlyPosixFileSystem(PosixFileSystem wrapped) {
		super(wrapped);
		this.wrapped = wrapped;
	}

	/**
	 * Delegates to the wrapped file system.
	 */
	@Override
	public Stat stat(Path path) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		return wrapped.stat(unwrapPath(path));
	}

	@Override
	public Path createFile(Path path, int mode) throws ReadOnlyFileSystemException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new ReadOnlyFileSystemException();
	}

	@Override
	public Path createDirectory(Path path, int mode) throws ReadOnlyFileSystemException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new ReadOnlyFileSystemException();
	}
}

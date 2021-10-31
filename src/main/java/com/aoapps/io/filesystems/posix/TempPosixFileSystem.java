/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2015, 2019, 2020, 2021  AO Industries, Inc.
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

import com.aoapps.io.filesystems.InvalidPathException;
import com.aoapps.io.filesystems.Path;
import com.aoapps.io.filesystems.TempFileSystem;
import com.aoapps.io.posix.Stat;
import org.apache.commons.lang3.NotImplementedException;

/**
 * A temporary Unix file system stored in the Java heap.
 *
 * @author  AO Industries, Inc.
 */
public class TempPosixFileSystem extends TempFileSystem implements PosixFileSystem {

	/**
	 * @see  PosixFileSystem#checkSubPath(com.aoapps.io.filesystems.Path, java.lang.String)
	 */
	@Override
	public void checkSubPath(Path parent, String name) throws InvalidPathException {
		PosixFileSystem.super.checkSubPath(parent, name);
	}

	@Override
	public Stat stat(Path path) {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new NotImplementedException("TODO");
	}

	@Override
	public Path createFile(Path path, int mode) {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new NotImplementedException("TODO");
	}

	@Override
	public Path createDirectory(Path path, int mode) {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new NotImplementedException("TODO");
	}
}

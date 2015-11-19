/*
 * ao-io-filesystems-unix - Advanced filesystem utilities for Unix.
 * Copyright (C) 2015  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-io-filesystems-unix.
 *
 * ao-io-filesystems-unix is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-io-filesystems-unix is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-io-filesystems-unix.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.io.filesystems.unix;

import com.aoindustries.io.filesystems.InvalidPathException;
import com.aoindustries.io.filesystems.Path;
import com.aoindustries.io.filesystems.TempFileSystem;
import com.aoindustries.io.unix.Stat;
import com.aoindustries.lang.NotImplementedException;
import java.io.IOException;

/**
 * A temporary Unix file system stored in the Java heap.
 *
 * @author  AO Industries, Inc.
 */
public class TempUnixFileSystem extends TempFileSystem implements UnixFileSystem {

	/**
	 * @see  UnixFileSystem#checkSubPath(com.aoindustries.io.filesystems.Path, java.lang.String)
	 */
	@Override
	public void checkSubPath(Path parent, String name) throws InvalidPathException {
		UnixFileSystem.super.checkSubPath(parent, name);
	}

	@Override
	public Stat stat(Path path) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new NotImplementedException("TODO");
	}
}

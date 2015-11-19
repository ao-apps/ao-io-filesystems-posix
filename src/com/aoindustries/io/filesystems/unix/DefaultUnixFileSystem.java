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
import com.aoindustries.io.filesystems.JavaFileSystem;
import com.aoindustries.io.filesystems.Path;
import com.aoindustries.io.unix.Stat;
import com.aoindustries.io.unix.UnixFile;
import java.io.IOException;

/**
 * The Unix file system implement by the UnixFile.
 * 
 * @see UnixFile
 *
 * @author  AO Industries, Inc.
 */
public class DefaultUnixFileSystem extends JavaFileSystem implements UnixFileSystem {

	private static final DefaultUnixFileSystem instance = new DefaultUnixFileSystem();

	/**
	 * Only one instance is created.
	 */
	public static DefaultUnixFileSystem getInstance() {
		return instance;
	}

	protected DefaultUnixFileSystem() {
	}

	/**
	 * @see  UnixFileSystem#checkSubPath(com.aoindustries.io.filesystems.Path, java.lang.String)
	 */
	@Override
	public void checkSubPath(Path parent, String name) throws InvalidPathException {
		UnixFileSystem.super.checkSubPath(parent, name);
	}

	/**
	 * Gets a UnixFile for the given path.
	 * 
	 * @see #getFile(com.aoindustries.io.filesystems.Path) 
	 */
	private UnixFile getUnixFile(Path path) throws IOException {
		assert path.getFileSystem() == this;
		return new UnixFile(getFile(path));
	}

	@Override
	public Stat stat(Path path) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		return getUnixFile(path).getStat();
	}
}

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

import com.aoapps.io.filesystems.InvalidPathException;
import com.aoapps.io.filesystems.JavaFileSystem;
import com.aoapps.io.filesystems.Path;
import com.aoapps.io.posix.PosixFile;
import com.aoapps.io.posix.Stat;
import java.io.IOException;
import java.nio.file.FileSystems;

/**
 * The Unix file system implement by the PosixFile.
 * 
 * @see PosixFile
 *
 * @author  AO Industries, Inc.
 */
public class DefaultPosixFileSystem extends JavaFileSystem implements PosixFileSystem {

	private static final DefaultPosixFileSystem instance = new DefaultPosixFileSystem();

	/**
	 * Only one instance is created.
	 */
	public static DefaultPosixFileSystem getInstance() {
		return instance;
	}

	protected DefaultPosixFileSystem() {
		super(FileSystems.getDefault());
		if(!isSingleRoot) throw new AssertionError("Default Unix filesystem must always be single root");
	}

	/**
	 * @see  PosixFileSystem#checkSubPath(com.aoapps.io.filesystems.Path, java.lang.String)
	 */
	@Override
	public void checkSubPath(Path parent, String name) throws InvalidPathException {
		PosixFileSystem.super.checkSubPath(parent, name);
	}

	/**
	 * Gets a PosixFile for the given path.
	 * 
	 * @see #getFile(com.aoapps.io.filesystems.Path)
	 */
	private PosixFile getPosixFile(Path path) throws IOException {
		assert path.getFileSystem() == this;
		return new PosixFile(getJavaPath(path).toFile());
	}

	@Override
	public Stat stat(Path path) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		return getPosixFile(path).getStat();
	}

	/**
	 * TODO: This is not an atomic implementation.  Use the Java-provided interface,
	 * but beware it does not seem to have support for the sticky bits.
	 */
	@Override
	public Path createFile(Path path, int mode) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		super.createFile(path);
		getPosixFile(path).setMode(mode);
		return path;
	}

	/**
	 * TODO: This is not an atomic implementation.  Use the Java-provided interface,
	 * but beware it does not seem to have support for the sticky bits.
	 */
	@Override
	public Path createDirectory(Path path, int mode) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		getPosixFile(path).mkdir(false, mode);
		return path;
	}
}

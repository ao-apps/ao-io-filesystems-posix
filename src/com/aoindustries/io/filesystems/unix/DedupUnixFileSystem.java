/*
 * ao-io-filesystems-unix - Advanced filesystem utilities for Unix.
 * Copyright (C) 2015, 2019  AO Industries, Inc.
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

import com.aoindustries.io.filesystems.FileSystemWrapper;
import com.aoindustries.io.filesystems.Path;
import com.aoindustries.io.unix.Stat;
import java.io.IOException;

/**
 * De-duplicates data chunks on the fly.
 *
 * <pre>
 * real name (empty file, small file, or possible full/partially restored large file)
 * &lt;A&lt;O&lt;DEDUP&gt;O&gt;A&gt;_# -> symbolic linked to real name
 * &lt;A&lt;O&lt;DEDUP&gt;O&gt;A&gt;_#_chunk#_md5_size[.gz] -> hard linked to index
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
public class DedupUnixFileSystem extends FileSystemWrapper implements UnixFileSystem {

	private final UnixFileSystem wrapped;

	public DedupUnixFileSystem(UnixFileSystem wrapped) {
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
	@SuppressWarnings("deprecation")
	public Path createFile(Path path) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new com.aoindustries.lang.NotImplementedException("TODO");
	}

	@Override
	@SuppressWarnings("deprecation")
	public Path createFile(Path path, int mode) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		throw new com.aoindustries.lang.NotImplementedException("TODO");
	}

	/**
	 * Delegates to the wrapped file system.
	 */
	@Override
	public Path createDirectory(Path path, int mode) throws IOException {
		if(path.getFileSystem() != this) throw new IllegalArgumentException();
		wrapped.createDirectory(unwrapPath(path), mode);
		return path;
	}
}

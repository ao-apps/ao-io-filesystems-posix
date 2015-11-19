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

import com.aoindustries.io.filesystems.FileSystem;
import com.aoindustries.io.filesystems.Path;
import com.aoindustries.io.unix.Stat;
import com.aoindustries.io.unix.UnixFile;
import java.io.IOException;

/**
 * The most basic layer of what Unix file systems have in common.
 * <p>
 * Note: The JVM must be in a single-byte locale, such as "C", "POSIX", or
 * "en_US".  UnixFile makes this assumption in its JNI implementation.
 * </p>
 * 
 * @see  UnixFile
 *
 * @author  AO Industries, Inc.
 */
public interface UnixFileSystem extends FileSystem {

	/**
	 * @path  Must be from this file system.
	 */
	Stat stat(Path path) throws IOException;
}

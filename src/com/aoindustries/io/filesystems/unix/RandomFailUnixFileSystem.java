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

import com.aoindustries.io.filesystems.RandomFailFileSystem;

/**
 * A Unix file system implementation that randomly fails, this is used by test
 * suites to verify correct behavior under expected failure modes.
 *
 * @author  AO Industries, Inc.
 */
public class RandomFailUnixFileSystem extends RandomFailFileSystem implements UnixFileSystem {

	private final UnixFileSystem wrapped;

	public RandomFailUnixFileSystem(UnixFileSystem wrapped) {
		super(wrapped);
		this.wrapped = wrapped;
	}
}

/*
 * ao-io-filesystems-posix - POSIX filesystem abstraction.
 * Copyright (C) 2021  AO Industries, Inc.
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
module com.aoapps.io.filesystems.posix {
	exports com.aoapps.io.filesystems.posix;
	// Direct
	requires com.aoapps.hodgepodge; // <groupId>com.aoapps</groupId><artifactId>ao-hodgepodge</artifactId>
	requires com.aoapps.io.filesystems; // <groupId>com.aoapps</groupId><artifactId>ao-io-filesystems</artifactId>
	requires com.aoapps.io.posix; // <groupId>com.aoapps</groupId><artifactId>ao-io-posix</artifactId>
	requires com.aoapps.lang; // <groupId>com.aoapps</groupId><artifactId>ao-lang</artifactId>
	requires static com.aoindustries.aoserv.daemon.client; // <groupId>com.aoindustries</groupId><artifactId>aoserv-daemon-client</artifactId>
	requires org.apache.commons.lang3; // <groupId>org.apache.commons</groupId><artifactId>commons-lang3</artifactId>
	// Java SE
	requires java.logging;
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DatabaseNotExistException;
import org.apache.flink.table.api.TableNotExistException;

import java.io.IOException;
import java.util.List;

/**
 * Catalog for testing.
 */
public class TestCatalog implements ReadableCatalog {

	@Override
	public void open() {

	}

	@Override
	public List<String> listDatabases() {
		return null;
	}

	@Override
	public CatalogDatabase getDatabase(String dbName) throws DatabaseNotExistException {
		return null;
	}

	@Override
	public List<ObjectPath> listTablesByDatabase(String dbName) throws DatabaseNotExistException {
		return null;
	}

	@Override
	public List<ObjectPath> listAllTables() {
		return null;
	}

	@Override
	public ExternalCatalogTable getTable(ObjectPath tableName) throws TableNotExistException {
		return null;
	}

	@Override
	public void close() throws IOException {

	}
}

[![Maven Central](https://maven-badges.sml.io/sonatype-central/org.pgcodekeeper/pgcodekeeper-core/badge.svg)](https://maven-badges.sml.io/sonatype-central/org.pgcodekeeper/pgcodekeeper-core)
[![Apache 2.0](https://img.shields.io/github/license/pgcodekeeper/pgcodekeeper-core.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# pgCodeKeeper-core

A tool for easier PostgreSQL, Greenplum, MS SQL, ClickHouse development.

* Comparing database code is very easy now. Compare live DB instances, pg_dump output, as well as pgCodeKeeper projects.
* Generate migration scripts via a user-friendly interface. You can use both live DB instances and DB dumps as initial data. You can also compare pgCodeKeeper projects — useful when working with versions control systems.

# Maven Repository

You can pull pgcodekeeper-core from the central maven repository, just add these to your pom.xml file:

```
<dependency>
    <groupId>org.pgcodekeeper</groupId>
    <artifactId>pgcodekeeper-core</artifactId>
    <version>{version}</version>
</dependency>
```

# Usage

Currently static methods are available:

```java
// Example 1: Compare two databases from JDBC connections and generate migration script
ISettings settings = new CoreSettings();
DiffSettings diffSettings = new DiffSettings(settings);

// settings init...

IDatabaseProvider databaseProvider = new PgDatabaseProvider();
IJdbcLoader oldDb = databaseProvider.getJdbcLoader("jdbc:postgresql://localhost/old_db", diffSettings);

IJdbcLoader newDb = databaseProvider.getJdbcLoader("jdbc:postgresql://localhost/new_db", diffSettings);

String migrationScript = PgCodeKeeperApi.diff(databaseProvider, oldDb, newDb, diffSettings);

// Example 2: Load database from project and compare with JDBC database
IDatabase projectDb = databaseProvider.getProjectLoader("/path/to/project", diffSettings).loadAndAnalyze();
IDatabase liveDb = databaseProvider.getJdbcLoader("jdbc:postgresql://localhost/live_db", diffSettings).loadAndAnalyze();
String script = PgCodeKeeperApi.diff(databaseProvider, projectDb, liveDb, diffSettings);

// Example 3: Compare databases with object filtering using ignore list

IDatabase db1 = databaseProvider.getJdbcLoader("jdbc:postgresql://localhost/db1", diffSettings).loadAndAnalyze();
IDatabase db2 = databaseProvider.getProjectLoader("/path/to/project", diffSettings).loadAndAnalyze();
diffSettings.addIgnoreList("/path/to/object_ignore_list.pgcodekeeperignore");

String diff = PgCodeKeeperApi.diff(databaseProvider, db1, db2, diffSettings);

// Example 4: Export database to project with filtering and progress monitoring
DiffSettings newDiffSettings = new DiffSettings(settings, new NullMonitor());

IJdbcLoader db = databaseProvider.getJdbcLoader("jdbc:postgresql://localhost/db", newDiffSettings);

diffSettings.addIgnoreList("/path/to/object_ignore_list.pgcodekeeperignore");
PgCodeKeeperApi.exportToProject(databaseProvider, null, db, "/path/to/export", diffSettings);

// Example 5: Update project with changes from database loader
IDumpLoader project = databaseProvider.getProjectLoader("/path/to/project", diffSettings);
IJdbcLoader updatedDb = databaseProvider.getJdbcLoader("jdbc:postgresql://localhost/updated_db", diffSettings);
PgCodeKeeperApi.exportToProject(databaseProvider, project, updatedDb, "/path/to/project", diffSettings);
```

## Documentation

* [User manual](https://pgcodekeeper.readthedocs.io/en/latest/)
* [Issue tracker](https://github.com/pgcodekeeper/pgcodekeeper-core/issues)

## Build

Build requires Java (JDK) 17+ and Apache Maven 3.9+.

## Notes

- If you have any questions, suggestions, ideas, etc - contact us in our [Telegram chat](https://t.me/pgcodekeeper) or create an issue.
- Pull requests are welcome.
- Visit https://pgcodekeeper.org for more information.
- Thanks for using pgCodeKeeper!

## Contributing

### Module Contents

This project was derived from [apgdiff](https://github.com/fordfrog/apgdiff) project. At this point it is almost fully rewritten according to our needs.

Primary packages of this module are:

- `org.pgcodekeeper.core.api` - high-level API classes for database operations.
- `org.pgcodekeeper.core.database` - main package for implementing database-specific logic. Includes the following subpackages (all have identical subpackages):
    - `api` - provides the high-level abstraction for database interactions.
    - `base` - contains reusable logic and base classes for all database providers.
    - {ch, pg, ms} - concrete implementations for specific databases.
- `org.pgcodekeeper.core.database.base.schema.meta` - simplified SQL object classes for database structure serialization.
- `org.pgcodekeeper.core.database.base.schema.meta` (resources) - serialized descriptions of system objects.
- `org.pgcodekeeper.core.database.*.jdbc` - contains database-specific classes that build schema objects from JDBC ResultSet.
- `org.pgcodekeeper.core.database.*.loader` - contains database schema loaders: project, SQL file, JDBC, library.
- `org.pgcodekeeper.core.database.*.parser.genarated` - contains generated ANTLR4 parser code.
- `org.pgcodekeeper.core.database.*.parser.statement` - contains classes that create schema objects based on parser-read data.
- `org.pgcodekeeper.core.database.*.parser.expr` - contains expression analysis classes: find dependencies in expressions and infer expression types for overloaded function call resolution.
- `org.pgcodekeeper.core.database.*.parser.launcher` - contains classes for expression analysis.
- `org.pgcodekeeper.core.database.*.project` - contains classes for database project updaters and model exporters, with common functionality.
- `org.pgcodekeeper.core.database.*.schema` - contains classes and interfaces that describe SQL objects.
- `org.pgcodekeeper.core.database.*.formatter` - contains code formatters.
- `org.pgcodekeeper.core.database.*.utils` - provides utility classes including identifier quoting utilities and system constants.
- `org.pgcodekeeper.core.model.difftree` - classes representing and creating an object diff tree.
- `org.pgcodekeeper.core.model.graph` - object dependency graph classes, built using JGraphT library.
- `org.pgcodekeeper.core.sql` - a categorized list of all PostgreSQL keywords. Generated from PostgreSQL source.
- `org.pgcodekeeper.core.ignorelist` - contains classes for managing ignore rules that filter database objects during schema comparison, with support for whitelist/blacklist patterns, rule precedence, and file-based parsing.
- `org.pgcodekeeper.core` - main package containing general stuff: e.g. string constants, utils and general-purpose classes.
- `src.main.antlr4.org.pgcodekeeper.core.database.*.parser.generated` - ANTLR4 parser sources for database dialects (PostgreSQL, MS SQL, ClickHouse) and custom syntaxes, including Ignore Lists and PostgreSQL privilege parsers.
These need to be built using your preferred ANTLR4 builder into `org.pgcodekeeper.core.database.*.parser.generated` package.

Majority of tests are here.

- `org.pgcodekeeper.core.api` - integration tests for the high-level API operations (diff, export, update) using real test databases and projects.
- `org.pgcodekeeper.core` - these test cases load old and new database schemas, generate a migration script and compare it to the expected diff file.
- `org.pgcodekeeper.core.it` - integration test utils.
- `org.pgcodekeeper.core.it.depcies.*` - database-specific integration tests here work similarly to simple diff tests above, except the concept of "user selection" is added. Migration script is built only with objects in `usr` files as starting points, other objects must be added to the script via dependency mechanism.
- `org.pgcodekeeper.core.it.loader.*` - these database-specific integration tests load simple schemas from files and compare loaded IDatabase objects with predefined ones.
- `org.pgcodekeeper.core.it.jdbs.*` - contains JDBC loader integration tests that use Docker Testcontainers.
- `org.pgcodekeeper.core.it.parser.*` - these tests simply parse test pieces of code to verify parser validity.
- `org.pgcodekeeper.core.utils.testcontainer` - provides utilities for managing Docker containers via Testcontainers to facilitate JDBC-based tests.

### Program Lifecycle

General program lifecycle goes as follows:
1. `ISettings` object is filled with operation parameters.
2. `DiffSettings` object is filled with ignore lists, ignore schema list parameters.
3. `IDatabase`s are loaded from requested sources, including their libraries and privilege overrides. Ignored schemas are skipped at this step.
   1. During the load dependencies of each object are found and recorded. Expressions are also analyzed to extract their dependencies including overloaded function calls.
   2. All parser and expression analysis operations are run in parallel using `AntlrParser.ANTLR_POOL` thread pool to speed up the process. Parallel operations are serialized by calling `finishLoaders` at the end of each loading process.
4. The diff tree (represented by root `TreeElement`) is created by comparing two `IDatabase`s.
5. The diff tree, now containing "user selection", is used to selectively update project files on disk, or to generate a migration script.
6. In latter case, each "selected" TreeElement is passed to `DepcyResolver` to generate script actions fulfilling the requested change, including actions on dependent objects. To do this, JGraphT object dependency graphs are built using dependency information found at the loading stage.
7. Generated actions are now converted into SQL code with some last-moment post-processing and filtering.
8. Generated SQL script is returned as a String for user to review and run on their database.

[![Maven Central](https://maven-badges.sml.io/sonatype-central/org.pgcodekeeper/pgcodekeeper-core/badge.svg)](https://maven-badges.sml.io/sonatype-central/org.pgcodekeeper/pgcodekeeper-core)
[![Apache 2.0](https://img.shields.io/github/license/pgcodekeeper/pgcodekeeper-core.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# pgCodeKeeper-core

A tool for easier PostgreSQL, GreenPlum, MS SQL, ClickHouse development.

* Comparing database code is very easy now. Compare live DB instances, pg_dump output, as well as pgCodeKeeper projects.
* Generate migration scripts via a user-friendly interface. You can use both live DB instances and DB dumps as initial data. You can also compare pgCodeKeeper projects — useful when working with versions control systems.

# Maven Repository

You can pull pgcodekeeper-core from the central maven repository, just add these to your pom.xml file:

```
<dependency>
    <groupId>org.pgcodekeeper</groupId>
    <artifactId>pgcodekeeper-core</artifactId>
    <version>11.0.0-rc.1</version>
</dependency>
```

# Usage

Currently static methods are available:

```java
// Example 1: Compare two databases from JDBC connections and generate migration script
ISettings settings = new CoreSettings(DatabaseType.PG);

// settings init...

AbstractDatabase oldDb = DatabaseFactory.loadFromJdbc(settings, "jdbc:postgresql://localhost/old_db");
AbstractDatabase newDb = DatabaseFactory.loadFromJdbc(settings, "jdbc:postgresql://localhost/new_db");
String migrationScript = PgCodeKeeperApi.diff(settings, oldDb, newDb);

// Example 2: Load database from project and compare with JDBC database
AbstractDatabase projectDb = DatabaseFactory.loadFromProject(settings, "/path/to/project");
AbstractDatabase liveDb = DatabaseFactory.loadFromJdbc(settings, "jdbc:postgresql://localhost/live_db");
String script = PgCodeKeeperApi.diff(settings, projectDb, liveDb);

// Example 3: Compare databases with object filtering using ignore list
AbstractDatabase db1 = DatabaseFactory.loadFromJdbc(settings, "jdbc:postgresql://localhost/db1");
AbstractDatabase db2 = DatabaseFactory.loadFromProject(settings, "/path/to/project");
String diff = PgCodeKeeperApi.diff(settings, db1, db2, List.of("/path/to/object_ignore_list.txt"));

// Example 4: Export database to project with filtering and progress monitoring
IMonitor monitor = new NullMonitor(); // or implement custom progress monitoring
AbstractDatabase db = DatabaseFactory.loadFromJdbc(settings, "jdbc:postgresql://localhost/db");
PgCodeKeeperApi.export(settings, db, "/path/to/export", List.of("/path/to/object_ignore_list.txt"), monitor);

// Example 5: Update project with changes from database
AbstractDatabase projectDb = DatabaseFactory.loadFromProject(settings, "/path/to/project");
AbstractDatabase updatedDb = DatabaseFactory.loadFromJdbc(settings, "jdbc:postgresql://localhost/updated_db");
PgCodeKeeperApi.update(settings, projectDb, updatedDb, "/path/to/project");

// Example 6: Update project with filtering and progress monitoring
PgCodeKeeperApi.update(settings, projectDb, updatedDb, "/path/to/project",
                       List.of("/path/to/object_ignore_list.txt"), 
                       monitor);
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
- `org.pgcodekeeper.core.schema` - contains classes that describe SQL objects. Each class contains object properties, creation SQL code generator, object comparison logic and ALTER code generator.
- `org.pgcodekeeper.core.schema.meta` - simplified SQL object classes for database structure serialization.
- `org.pgcodekeeper.core.schema.meta` (resources) - serialized descriptions of PostgreSQL's system objects (built using JdbcSystemLoader).
- `org.pgcodekeeper.core.loader` - database schema loaders: project, SQL file, JDBC, library.
- `org.pgcodekeeper.core.loader` (resources) - SQL queries for JdbcLoaders, used by Java code via `JdbcQueries`.
- `org.pgcodekeeper.core.loader.jdbc` - classes that create schema objects based on JDBC ResultSets.
- `org.pgcodekeeper.core.parsers.antlr.*` - generated ANTLR4 parser code, plus utility parser classes.
- `org.pgcodekeeper.core.parsers.antlr.*.statements` - processor classes that create schema objects based on parser-read data.
- `org.pgcodekeeper.core.parsers.antlr.*.expr` - expression analysis classes: find dependencies in expressions and infer expression types for overloaded function call resolution.
- `org.pgcodekeeper.core.parsers.antlr.*.expr.launcher` - support for parallel expression analysis.
- `org.pgcodekeeper.core.formatter` - SQL and pl/pgsql code formatters.
- `org.pgcodekeeper.core.model.difftree` - classes representing and creating an object diff tree.
- `org.pgcodekeeper.core.model.graph` - object dependency graph classes, built using JGraphT library.
- `org.pgcodekeeper.core.model.exporter` - classes that save and update the project files on disk.
- `org.pgcodekeeper.core.sql` - a categorized list of all PostgreSQL keywords. Generated from PostgreSQL source.
- `org.pgcodekeeper.core.ignoreparser` - builder for `IgnoreList`s.
- `org.pgcodekeeper.core` - main package containing general stuff: e.g. string constants, utils and general-purpose classes.
- `src.main.antlr4.org.pgcodekeeper.core.parsers.antlr.generated` - sources for ANTLR4 parsers. We maintain parsers for PostgreSQL, pl/pgsql, T-SQL, and also our custom Ignore Lists and PostgreSQL ACLs syntax.  
These need to be built using your preferred ANTLR4 builder into `org.pgcodekeeper.core.parsers.antlr.*` package.

Majority of tests are here.

- `org.pgcodekeeper.core.api` - integration tests for the high-level API operations (diff, export, update) using real test databases and projects.
- `org.pgcodekeeper.core` - these test cases load old and new database schemas, generate a migration script and compare it to the expected diff file.
- `org.pgcodekeeper.core.depcies` - tests here work similarly to simple diff tests above, except the concept of "user selection" is added. Migration script is built only with objects in `usr` files as starting points, other objects must be added to the script via dependency mechanism.
- `org.pgcodekeeper.core.loader` - these tests load simple schemas from files and compare loaded AbstractDatabase objects with predefined ones.
- `org.pgcodekeeper.core.parsers` - these tests simply parse test pieces of code (taken from PostgreSQL source) to verify parser validity.
- `org.pgcodekeeper.core.parsers.antlr.expr` - these tests verify expression analysis and type inference mechanism.
- `org.pgcodekeeper.core.model.exporter` - these test update exported project directories and verify which files have actually been changed.

### Program Lifecycle

General program lifecycle goes as follows:
1. `ISettings` object is filled with operation parameters.
2. `AbstractDatabase`s are loaded from requested sources, including their libraries and privilege overrides. Ignored schemas are skipped at this step.
   1. During the load dependencies of each object are found and recorded. Expressions are also analyzed to extract their dependencies including overloaded function calls.
   2. All parser and expression analysis operations are run in parallel using `AntlrParser.ANTLR_POOL` thread pool to speed up the process. Parallel operations are serialized by calling `finishLoaders` at the end of each loading process.
3. The diff tree (represented by root `TreeElement`) is created by comparing two `AbstractDatabase`s.
4. The diff tree, now containing "user selection", is used to selectively update project files on disk, or to generate a migration script.
5. In latter case, each "selected" TreeElement is passed to `DepcyResolver` to generate script actions fulfilling the requested change, including actions on dependent objects. To do this, JGraphT object dependency graphs are built using dependency information found at the loading stage.
6. Generated actions are now converted into SQL code with some last-moment post-processing and filtering.
7. Generated SQL script is returned as a String for user to review and run on their database.

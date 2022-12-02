[![Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![](https://jitpack.io/v/pgcodekeeper/pgcodekeeper-core.svg)](https://jitpack.io/#pgcodekeeper/pgcodekeeper-core)

# pgCodeKeeper-core

The core pgCodeKeeper module containing database schema classes and loaders, comparison logic, SQL code generators and parsers.

## Documentation

* [Issue tracker](https://github.com/pgcodekeeper/pgcodekeeper-core/issues)

## Build

You can add pgcodekeeper-core as a dependency from the JitPack repository:

```xml
  <repositories>
    <repository>
      <id>jitpack.io</id>
      <url>https://jitpack.io</url>
    </repository>
  </repositories>

  <dependencies>
    ...
    <dependency>
      <groupId>com.github.pgcodekeeper</groupId>
      <artifactId>pgcodekeeper-core</artifactId>
      <version>version</version>
    </dependency>
    ...
  </dependencies>
```

## Notes

- If you have any questions, suggestions, ideas, etc - contact us in our [Telegram chat](https://t.me/pgcodekeeper) or create an issue.
- Pull requests are welcome.
- Visit https://pgcodekeeper.org for more information.
- Thanks for using pgCodeKeeper!

## Contributing

### Project Structure

This module was derived from [apgdiff](https://github.com/fordfrog/apgdiff) project. At this point it is almost fully rewritten according to our needs.
Primary packages of this module are:

- `ru.taximaxim.codekeeper.core.schema` - contains classes that describe SQL objects. Each class contains object properties, creation SQL code generator, object comparison logic and ALTER code generator. Notable classes:
  - `PgStatement` - all object classes must be derived from this one. Defines main API for working with objects.
  - `PgDatabase` - root class for every loaded database schema.
  - `GenericColumn` - generic reference to any object (used to be a column reference).
- `ru.taximaxim.codekeeper.core.schema.meta` - simplified SQL object classes for database structure serialization.
- `ru.taximaxim.codekeeper.core.schema.meta` (resources) - serialized descriptions of PostgreSQL's system objects (built using JdbcSystemLoader).
- `ru.taximaxim.codekeeper.core.loader` - database schema loaders: project, SQL file, JDBC, library.
  - `ProjectLoader` - loads a database from project structure.
  - `PgDumpLoader` - loads objects into a database structure from a file.
- `ru.taximaxim.codekeeper.core.loader` (resources) - SQL queries for JdbcLoaders, used by Java code via `JdbcQueries`.
- `ru.taximaxim.codekeeper.core.loader.jdbc` - classes that create schema objects based on JDBC ResultSets.
- `ru.taximaxim.codekeeper.core.parsers.antlr` - generated ANTLR4 parser code, plus utility parser classes. Notable classes:
  - `AntlrParser` - factory methods for parser creation and parallel parser operations.
  - `QNameParser` - utility class for parsing and splitting qualified names.
  - `ScriptParser` - simple parser that splits statements for later execution.
  - `Custom(T)SQLParserListener` - main entry point for parsing SQLs into schema objects.
- `ru.taximaxim.codekeeper.core.parsers.antlr.statements` - processor classes that create schema objects based on parser-read data. Notable classes:
  - `ParserAbstract` - common base class for all processors.
- `ru.taximaxim.codekeeper.core.parsers.antlr.expr` - expression analysis classes: find dependencies in expressions and infer expression types for overloaded function call resolution.
- `ru.taximaxim.codekeeper.core.parsers.antlr.expr.launcher` - support for parallel expression analysis.
- `ru.taximaxim.codekeeper.core.formatter` - SQL and pl/pgsql code formatters.
- `ru.taximaxim.codekeeper.core` - util and general-purpose classes. Notable classes:
  - `PgDiff` - entry point for database schema diff operations.
- `ru.taximaxim.codekeeper.core.model.difftree` - classes representing and creating an object diff tree. Notable classes:
  - `DbObjType` - enum of all SQL object types supported by the program.
  - `TreeElement` - a node in an object diff tree.
  - `DiffTree` - object diff tree factory.
  - `TreeFlattener` - filters and flattens the tree into a list of nodes according to requested parameters.
- `ru.taximaxim.codekeeper.core.model.graph` - object dependency graph classes, built using JGraphT library. Notable classes:
  - `DepcyGraph` - the dependency graph and its builder.
  - `DepcyResolver` - main script building algorithm. Creates a list of `StatementAction`s according to requested object changes and object dependencies.
  - `DepcyTreeExtender` - extends an object diff tree according to object dependencies.
  - `DepcyWriter` - prints a dependency tree for an object.
- `ru.taximaxim.codekeeper.core.model.exporter` - classes that save and update the project files on disk. Notable classes:
  - `ModelExporter`, `MsModelExporter`, `AbstractModelExporter` - exporters for PG and MS projects and their common abstract part.
  - `OverridesModelExporter` - exports project structure containing only permission overrides.
- `ru.taximaxim.codekeeper.core.sql` - a categorized list of all PostgreSQL keywords. Generated from PostgreSQL source.
- `ru.taximaxim.codekeeper.core.ignoreparser` - builder for `IgnoreList`s.
- `ru.taximaxim.codekeeper.core` - main package containing general stuff: e.g. string constants, utils.
- `antlr-src` - sources for ANTLR4 parsers. We maintain parsers for PostgreSQL, pl/pgsql, T-SQL, and also our custom Ignore Lists and PostgreSQL ACLs syntax.
These need to be built using your preferred ANTLR4 builder into `ru.taximaxim.codekeeper.core.parsers.antlr` package.

#### tests

- `ru.taximaxim.codekeeper.core` - `PgDiffTest`, `MsDiffTest` - these test cases load old and new database schemas, generate a migration script and compare it to the expected diff file.
- `ru.taximaxim.codekeeper.core.depcies` - tests here work similarly to simple diff tests above, except the concept of "user selection" is added. Migration script is built only with objects in `usr` files as starting points, other objects must be added to the script via dependency mechanism.
- `ru.taximaxim.codekeeper.core.loader` - `PgAntlrLoaderTest`, `MsAntlrLoaderTest` - these tests load simple schemas from files and compare loaded PgDatabase objects with predefined ones.
- `ru.taximaxim.codekeeper.core.parsers` - these tests simply parse test pieces of code (taken from PostgreSQL source) to verify parser validity.
- `ru.taximaxim.codekeeper.core.parsers.antlr.expr` - these tests verify expression analysis and type inference mechanism.
- `ru.taximaxim.codekeeper.core.model.exporter` - these test update exported project directories and verify which files have actually been changed.

### Program Lifecycle

General program lifecycle goes as follows:
1. `PgDiffArguments` object is filled with operation parameters.
2. `PgDatabase`s are loaded from requested sources, including their libraries and privilege overrides. Ignored schemas are skipped at this step.
   1. During the load dependencies of each object are found and recorded. Expressions are also analyzed to extract their dependencies including overloaded function calls.
   2. All parser and expression analysis operations are run in parallel using `AntlrParser.ANTLR_POOL` thread pool to speed up the process. Parallel operations are serialized by calling `finishLoaders` at the end of each loading process.
3. The diff tree (represented by root `TreeElement`) is created by comparing two `PgDatabase`s. In GUI this is then shown to the user to assess the situation and choose objects to work with.
4. The diff tree, now containing "user selection", is used to selectively update project files on disk, or to generate a migration script.
5. In latter case, each "selected" TreeElement is passed to `DepcyResolver` to generate script actions fulfilling the requested change, including actions on dependent objects. To do this, JGraphT object dependency graphs are built using dependency information found at the loading stage.
6. Generated actions are now converted into SQL code with some last-moment post-processing and filtering.
7. Generated SQL script is written to a file/stdout.

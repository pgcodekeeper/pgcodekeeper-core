/*******************************************************************************
 * Copyright 2017-2026 TAXTELECOM, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.pgcodekeeper.core.dependencieslist;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import org.pgcodekeeper.core.TestUtils;

class DependenciesReaderTest {

    private static List<Dependency> result;

    @BeforeAll
    static void setUp() {
        Path path = TestUtils.getFilePath("deps.pgcodekeeperdependencies", DependenciesReaderTest.class);
        result = DependenciesReader.getDependencies(path);
    }

    private static Stream<Arguments> dependenciesTestData() {
        return Stream.of(
            Arguments.of(0, "emp_view", "emp", "VIEW", "TABLE"),
            Arguments.of(1, "user_code(integer)", "pr(boolean, boolean)", "FUNCTION", "FUNCTION"),
            Arguments.of(2, "the_part", "the_part_index_c1_idx", "TABLE", "INDEX"),
            Arguments.of(3, "ext1", "dummy", "EXTENSION", "FOREIGN DATA WRAPPER"),
            Arguments.of(4, "public", "emp", "SCHEMA", "TABLE"),
            Arguments.of(5, "calculate_rectangle_area(NUMERIC(10), NUMERIC(10,2))",
                    "validate_decimal(boolean)", "FUNCTION", "FUNCTION"),
            Arguments.of(6, "the_part", "col1", "TABLE", "COLUMN")
                );
    }

    @ParameterizedTest
    @MethodSource("dependenciesTestData")
    void testReadDependenciesCorrectly(int index, String expectedSource, String expectedTarget,
            String expectedSourceType, String expectedTargetType) {
        var entry = result.get(index);

        Assertions.assertEquals(expectedSource, entry.source().getName());
        Assertions.assertEquals(expectedTarget, entry.target().getName());
        Assertions.assertEquals(expectedSourceType, entry.source().type().getTypeName());
        Assertions.assertEquals(expectedTargetType, entry.target().type().getTypeName());
    }
}

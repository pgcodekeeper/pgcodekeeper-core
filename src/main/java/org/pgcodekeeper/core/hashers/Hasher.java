/*******************************************************************************
 * Copyright 2017-2025 TAXTELECOM, LLC
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
package org.pgcodekeeper.core.hashers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Hasher {
    void put(boolean b);
    void put(Boolean b);
    void put(String s);
    void put(float f);
    void put(int i);
    void put(Integer i);
    void put(IHashable hashable);
    void put(Enum<?> en);
    void put(Map<String, String> map);
    void put(List<String> col);
    void put(Set<String> col);
    void putOrdered(Collection<? extends IHashable> col);
    void putUnordered(Collection<? extends IHashable> col);
    void putUnordered(Map<String, ? extends IHashable> map);
}

/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.test.integration.core.unitofwork;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import net.helenus.test.integration.core.unitofwork.FilesystemNode;
import org.junit.BeforeClass;
import org.junit.Test;


public class UnitOfWorkTest extends AbstractEmbeddedCassandraTest {

    static FilesystemNode node;

    static HelenusSession session;

    @BeforeClass
    public static void beforeTest() {
        session = Helenus.init(getSession())
                .showCql()
                .add(FilesystemNode.class)
                .autoCreateDrop()
                .get();
        node = session.dsl(FilesystemNode.class);
    }


/*
    @Test
    public void testCruid() throws Exception {
        session.insert()
                .value(widgets::id, UUIDs.timeBased())
                .value(widgets::name, RandomString.make(20))
                .sync(uow5);
    }
*/
}

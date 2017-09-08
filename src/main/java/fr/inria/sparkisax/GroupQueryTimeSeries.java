/*
 * Copyright 2016 Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.inria.sparkisax;

import java.util.Collection;

import org.apache.spark.api.java.function.Function;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class GroupQueryTimeSeries implements Function<Iterable<iSAXWord>, iSAXWord[]> {

    public int size(Iterable<?> it) {
        if(it instanceof Collection) {
            return ((Collection<?>) it).size();
        }

        // else iterate
        int i = 0;
        for(Object obj : it) {
            i++;
        }
        return i;
    }

    @Override
    public iSAXWord[] call(Iterable<iSAXWord> v1) throws Exception {

        iSAXWord[] tsMat = new iSAXWord[size(v1)];
        int        i     = 0;
        for(iSAXWord ts : v1) {
            tsMat[i] = ts;
            i++;
        }

        return tsMat;
    }

}

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

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class CustomComparator implements Comparator, Serializable {

    @Override
    public int compare(Object o1, Object o2) {

        short[] tab1 = (short[]) o1;
        short[] tab2 = (short[]) o2;
        for(int i = 0; i < tab1.length; i++) {
            if(tab1[i] > tab2[i]) {
                return 1;
            } else {
                if(tab1[i] < tab2[i]) {
                    return -1;
                }
            }
        }

        return 0;
    }
}

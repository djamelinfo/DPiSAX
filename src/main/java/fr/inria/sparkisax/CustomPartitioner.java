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
import java.util.Map;

import org.apache.spark.Partitioner;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class CustomPartitioner extends Partitioner implements Serializable {

    private static final long serialVersionUID = 1L;
    private Map<String, Integer> partitions;

    public CustomPartitioner(Map<String, Integer> noOfPartitioners) {
        partitions = noOfPartitioners;
    }

    @Override
    public int getPartition(Object key) {

        short[] tab = (short[]) key;
        String  str = "";
        for(short aTab : tab) {
            str += (aTab >>> 8);
        }
        for(Map.Entry<String, Integer> entrySet : partitions.entrySet()) {
            if(str.startsWith(entrySet.getKey())) {
                return entrySet.getValue();
            }

        }
        return 0;

    }

    @Override
    public int numPartitions() {
        return partitions.size();
    }
}

package fr.inria.sparkisax;

import java.io.Serializable;
import java.util.Map;

import org.apache.spark.Partitioner;

/*
 * Copyright 2016 djamel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * @author djamel
 */
public class iSAXPartitioner extends Partitioner implements Serializable {

    private static final long serialVersionUID = 1L;
    private Map<String, iSAXPartition> partitions;

    public iSAXPartitioner(Map<String, iSAXPartition> noOfPartitioners) {
        partitions = noOfPartitioners;
    }

    @Override
    public int getPartition(Object key) {

        short[] tab = (short[]) key;

        for(Map.Entry<String, iSAXPartition> entrySet : partitions.entrySet()) {
            String        key1  = entrySet.getKey();
            iSAXPartition value = entrySet.getValue();
            String        str   = "";
            for(int i = 0; i < tab.length; i++) {
                if(value.getSaxCard()[i] == 9) {
                    str += "-" + " ";
                } else {
                    str += (tab[i] >>> value.getSaxCard()[i]) + "_" + value.
                                                                                   getSaxCard()[i] + " ";
                }
            }

            if(key1.equalsIgnoreCase(str)) {
                return value.getPartitionID();
            }

        }

        return 0;

    }

    @Override
    public int numPartitions() {
        return partitions.size();
    }
}

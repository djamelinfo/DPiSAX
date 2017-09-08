package fr.inria.sparkisax;

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

import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Map;

/**
 * @author djamel
 */
public class basiciSAXPartitioner extends Partitioner implements Serializable {

    private static final long serialVersionUID = 1L;
    private Map<String, iSAXPartition> partitions;

    public basiciSAXPartitioner(Map<String, iSAXPartition> noOfPartitioners) {
        partitions = noOfPartitioners;
    }

    @Override
    public int getPartition(Object key) {

        String key2= (String) key;

        if(partitions.containsKey(key2))
            return partitions.get(key2).getPartitionID();

        return this.partitions.size();

    }

    @Override
    public int numPartitions() {
        return partitions.size();
    }
}

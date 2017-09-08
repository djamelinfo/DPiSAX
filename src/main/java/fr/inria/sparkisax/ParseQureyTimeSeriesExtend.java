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

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class ParseQureyTimeSeriesExtend implements PairFunction<Tuple2<Integer, float[]>, Integer, iSAXWord> {

    private final boolean                    normalization;
    private final int                        wordLen;
    private final Map<String, iSAXPartition> partitions;

    public ParseQureyTimeSeriesExtend(boolean normalization, int wordLen, Map<String, iSAXPartition>    partitions) {
        this.normalization = normalization;
        this.wordLen = wordLen;
        this.partitions = partitions;
    }


    private Integer getPartitionIs(short[] saxWord){


        for(Map.Entry<String, iSAXPartition> entrySet : partitions.entrySet()) {
            String        key1  = entrySet.getKey();
            iSAXPartition value = entrySet.getValue();
            String        str   = "";
            for(int i = 0; i < saxWord.length; i++) {
                if(value.getSaxCard()[i] == 9) {
                    str += "-" + " ";
                } else {
                    str += (saxWord[i] >>> value.getSaxCard()[i]) + "_" + value.getSaxCard()[i] + " ";
                }
            }

            if(key1.equalsIgnoreCase(str)) {
                return value.getPartitionID();
            }

        }

        return this.partitions.size();
    }

    @Override
    public Tuple2<Integer, iSAXWord> call(Tuple2<Integer, float[]> t) throws Exception {

        short[] saxWord;


        if(this.normalization) {
           
            saxWord = SAXutils.ConvertSAX(SAXutils.Z_Normalization(t._2), this.wordLen);
        } else {

            saxWord = SAXutils.ConvertSAX(t._2, this.wordLen);
        }


        int partitionId = this.getPartitionIs(saxWord);


        return new Tuple2<>(partitionId, new iSAXWord(saxWord, t._1));



    }

}

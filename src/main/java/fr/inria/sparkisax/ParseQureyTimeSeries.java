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

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class ParseQureyTimeSeries implements PairFunction<Tuple2<Integer, float[]>, String, iSAXWord> {

    private final boolean normalization;
    private final int     wordLen;

    public ParseQureyTimeSeries(boolean normalization, int wordLen) {
        this.normalization = normalization;
        this.wordLen = wordLen;
    }

    @Override
    public Tuple2<String, iSAXWord> call(Tuple2<Integer, float[]> t) throws Exception {

        short[] saxWord;
        String  str = "";

        if(this.normalization) {

            saxWord = SAXutils.ConvertSAX(SAXutils.Z_Normalization(t._2), this.wordLen);
        } else {

            saxWord = SAXutils.ConvertSAX(t._2, this.wordLen);
        }

        for(short aSaxWord : saxWord) {
            str += (aSaxWord >>> 8);
        }

        return new Tuple2<>(str, new iSAXWord(saxWord, t._1));
    }

}
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
public class ParseTimeSeries implements PairFunction<Tuple2<Integer, float[]>, short[], Integer> {

    private final boolean normalization;
    private final int     wordLen;

    public ParseTimeSeries(boolean normalization, int wordLen) {
        this.normalization = normalization;
        this.wordLen = wordLen;
    }

    @Override
    public Tuple2<short[], Integer> call(Tuple2<Integer, float[]> t) throws Exception {

        if(this.normalization) {
            return new Tuple2<>(SAXutils.ConvertSAX(SAXutils.Z_Normalization(t._2), this.wordLen), t._1);
        } else {
            return new Tuple2<>(SAXutils.ConvertSAX(t._2, this.wordLen), t._1);
        }
    }

}

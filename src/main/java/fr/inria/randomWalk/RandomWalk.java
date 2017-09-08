/*
 * Copyright 2016  Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>..
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
package fr.inria.randomWalk;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaVectorRDD;

import scala.Tuple2;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class RandomWalk {

    public static JavaPairRDD<Integer, float[]> GeneratedRandomWalkData(JavaSparkContext jsc, long l, int i, int p) {

        JavaRDD<Vector> rdd = normalJavaVectorRDD(jsc, l, i, p);

        return rdd.zipWithUniqueId().mapToPair(new PairFunction<Tuple2<Vector, Long>, Integer, float[]>() {

            @Override
            public Tuple2<Integer, float[]> call(Tuple2<Vector, Long> t1) throws Exception {
                double[] tstemp = t1._1.toArray();

                float[] ts = new float[tstemp.length];
                ts[0] = (float) tstemp[0];

                for(int i = 1; i < tstemp.length; i++) {
                    tstemp[i] = tstemp[i - 1] + tstemp[i];
                    ts[i] = (float) tstemp[i];
                }

                return new Tuple2<>(t1._2.intValue(), ts);
            }
        });

    }

}

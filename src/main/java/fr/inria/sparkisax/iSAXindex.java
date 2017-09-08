/*
 * Copyright 2016 djamel.
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
import java.util.ArrayList;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author djamel
 */
public abstract class iSAXindex implements Serializable, Cloneable {

    protected Configuration                 config;
    protected Map<String, iSAXPartition>    partitions;
    public    JavaPairRDD<Integer, float[]> timeSeries;

    public iSAXindex(Configuration config) {
        this.config = config;
    }

    public Map<String, iSAXPartition> getPartitions() {
        return partitions;
    }

    public Configuration getConfig() {
        return config;
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }

    /**
     * @param timeSeriesRDD
     * @param a
     * @throws java.lang.CloneNotSupportedException
     */
    public void index(JavaPairRDD<Integer, float[]> timeSeriesRDD, int a,JavaSparkContext sc) throws Exception {

    }

    public void getStat() {

    }


    public void getStat2() {

    }

    public void printIndexRoots(){}

    public JavaPairRDD<Integer, Tuple2<float[], Integer>> ApproximateSearch(JavaRDD<String> Query, JavaSparkContext sc) {
        return null;
    }






    public JavaPairRDD<Integer, ArrayList<Integer>> ApproximateSearch(JavaPairRDD<Integer, float[]> QuerytimeSeries, JavaSparkContext sc) {
        return null;
    }

    public void ExactSearch(JavaRDD<float[]> QuerytimeSeries) {

    }

    public void saveAsTextFile(String path) {

    }

    public void saveAsObjectFile(String path) {

    }

    public String toDebugString() {
        return null;
    }

    public void print(){

    }


}

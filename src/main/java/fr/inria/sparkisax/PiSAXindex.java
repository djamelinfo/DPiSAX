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


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class PiSAXindex extends iSAXindex {

    JavaPairRDD<Integer, AbstractNode> finalRDD;
    JavaRDD<AbstractNode> finalRDD2;


    public PiSAXindex(Configuration config) {
        super(config);
    }

    /**
     * @return the config
     */
    @Override
    public Configuration getConfig() {
        return config;
    }

    /**
     * @return the partitions
     */
    @Override
    public Map<String, iSAXPartition> getPartitions() {
        return partitions;
    }

    /**
     * @param timeSeriesRDD
     * @param a
     * @throws java.lang.CloneNotSupportedException
     */
    @Override
    public void index(JavaPairRDD<Integer, float[]> timeSeriesRDD, int a, JavaSparkContext sc) throws Exception {

        this.timeSeries = timeSeriesRDD;/*
         * .persist(StorageLevel. MEMORY_AND_DISK_SER());
         */

        JavaPairRDD<short[], Integer> tsRDD = timeSeries.mapToPair(new ParseTimeSeries(config.isNormalization(), config.getWordLen())).persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<short[]> simpleRdd = tsRDD.sample(false, config.getFraction()).keys();

        List<short[]> listOfSAmples = simpleRdd.collect();

        switch(a) {
            case 1:
                // commented to add som optimisation and to correct iSAX index bug
                partitions = PartitionsUtil.getiSAXPartitionsWithCard(listOfSAmples, config);
                //finalRDD = tsRDD.repartitionAndSortWithinPartitions(new iSAXPartitioner(partitions), new
                //        CustomComparator()).mapPartitions(new Index(config), true).persist(StorageLevel.MEMORY_ONLY
                // ());
                //***********

                final Broadcast<Map<String, iSAXPartition>> partitionTable = sc.broadcast(partitions);
                JavaPairRDD<String, iSAXWord> customRddForPartitioning = tsRDD.mapToPair(new PairFunction<Tuple2<short[], Integer>, String, iSAXWord>() {


                    @Override
                    public Tuple2<String, iSAXWord> call(Tuple2<short[], Integer> integerTuple2) throws Exception {

                        Map<String, iSAXPartition> saxPartitionHashMap = partitionTable.value();

                        for(Map.Entry<String, iSAXPartition> entrySet : partitions.entrySet()) {
                            String        key1  = entrySet.getKey();
                            iSAXPartition value = entrySet.getValue();
                            String        str   = "";
                            for(int i = 0; i < integerTuple2._1().length; i++) {
                                if(value.getSaxCard()[i] == 9) {
                                    str += "-" + " ";
                                } else {
                                    str += (integerTuple2._1()[i] >>> value.getSaxCard()[i]) + "_" + value.getSaxCard()[i] + " ";
                                }
                            }

                            if(key1.equalsIgnoreCase(str)) {
                                return new Tuple2<String, iSAXWord>(str, new iSAXWord(integerTuple2._1(), integerTuple2._2()));
                            }

                        }

                        return new Tuple2<String, iSAXWord>("p", new iSAXWord(integerTuple2._1(), integerTuple2._2()));
                    }
                }).repartitionAndSortWithinPartitions(new basiciSAXPartitioner(partitions));


                finalRDD = customRddForPartitioning.mapPartitionsToPair(new ExtendIndex(this.getConfig(), partitions));


                break;
            case 2:
                partitions = PartitionsUtil.getiSAXPartitions(listOfSAmples, config);
                finalRDD2 = tsRDD.repartitionAndSortWithinPartitions(new iSAXPartitioner(partitions), new
                    CustomComparator()).mapPartitions(new Index(config), true).persist(StorageLevel.MEMORY_ONLY());
                break;
            case 3:
                partitions = PartitionsUtil.getPartitions(listOfSAmples, config.getPartitions());
                finalRDD2 = tsRDD.repartitionAndSortWithinPartitions(new iSAXPartitioner(partitions), new
                        CustomComparator()).mapPartitions(new Index(config), true).persist(StorageLevel.MEMORY_ONLY());
                break;
        }


        System.out.println(simpleRdd.toDebugString());

        ////////////////////////////////////////////////////////////////////////////
    }

    @Override
    public void getStat() {
        JavaPairRDD<Integer, String> rdd = finalRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,AbstractNode>>, Integer, String>() {

            @Override
            public Iterable<Tuple2<Integer, String>> call(Iterator<Tuple2<Integer,AbstractNode>> t) throws Exception {

                ArrayList<Tuple2<Integer, String>> liste = new ArrayList<>();
                int                                sum   = 0;
                int                                pro   = 0;
                int                                nbrT  = 0;
                while(t.hasNext()) {

                    AbstractNode b = t.next()._2();
                    sum += b.getNbrOfNode();
                    int a = b.getPro();
                    if(pro < a) {
                        pro = a;
                    }
                    nbrT += b.getNbrTs();
                }

                liste.add(new Tuple2<>(pro, sum + "," + nbrT));

                return liste;
            }
        });

        List<Tuple2<Integer, String>> p = rdd.collect();

        for(Tuple2<Integer, String> p1 : p) {
            System.out.println(p1._1 + "," + p1._2);
        }
    }


    @Override
    public void getStat2() {
        JavaPairRDD<Integer, String> rdd = finalRDD2.mapPartitionsToPair(new
                                                                                 PairFlatMapFunction<Iterator<AbstractNode>, Integer, String>() {

            @Override
            public Iterable<Tuple2<Integer, String>> call(Iterator<AbstractNode> t) throws Exception {

                ArrayList<Tuple2<Integer, String>> liste = new ArrayList<>();
                int                                sum   = 0;
                int                                pro   = 0;
                int                                nbrT  = 0;
                while(t.hasNext()) {

                    AbstractNode b = t.next();
                    sum += b.getNbrOfNode();
                    int a = b.getPro();
                    if(pro < a) {
                        pro = a;
                    }
                    nbrT += b.getNbrTs();
                }

                liste.add(new Tuple2<>(pro, sum + "," + nbrT));

                return liste;
            }
        });

        List<Tuple2<Integer, String>> p = rdd.collect();

        for(Tuple2<Integer, String> p1 : p) {
            System.out.println(p1._1 + "," + p1._2);
        }
    }

    public void printIndexRoots() {
        List<Tuple2<String, String>> p = finalRDD.mapToPair(new PairFunction<Tuple2<Integer,AbstractNode>, String, String>() {


            @Override
            public Tuple2<String, String> call(Tuple2<Integer,AbstractNode> abstractNode) throws Exception {
                byte[]  card  = abstractNode._2().getSaxCard();
                short[] words = abstractNode._2().getSaxWord();

                String str1 = "";
                String str2 = "";

                for(int i = 0; i < card.length; i++) {

                    str1 += card[i] + "";

                    str2 += words[i] + "|";


                }


                return new Tuple2<String, String>(str1, str2);
            }
        }).collect();

        for(Tuple2<String, String> p1 : p) {
            System.out.println(p1);
        }


    }

    @Override
    public JavaPairRDD<Integer, Tuple2<float[], Integer>> ApproximateSearch(JavaRDD<String> Query, JavaSparkContext sc) {



        //        HashMap<Integer, Integer> IDsMap = new HashMap<>();
        //----------------------------------------------------------------------------------------------------------------
        //        IDsMap.putAll(finalRDD.
        //                flatMapToPair(
        //                        new PairFlatMapFunction<AbstractNode, Integer, Integer>() {
        //
        //                            @Override
        //                            public Iterable<Tuple2<Integer, Integer>> call(
        //                                    AbstractNode t)
        //                            throws
        //                            Exception {
        //                                List<Tuple2<Integer, Integer>> l
        //                                                                       = new ArrayList<>();
        //                                iSAXWord[] s = q.value().get(t.
        //                                        getHashCode());
        //
        //                                if(s != null) {
        //                                    for(iSAXWord s1 : s) {
        //                                        int[] tempTab = t.
        //                                        ApproximateSearch(
        //                                                s1.saxWord);
        //
        //                                        if(tempTab != null) {
        //                                            int j = 0;
        //                                            for(int i = 0; i
        //                                            < tempTab.length;
        //                                            i++) {
        //
        //                                                if(tempTab[i] != 0 && j
        //                                                < 10) {
        //                                                    l.
        //                                                    add(new Tuple2<>(
        //                                                                    tempTab[i],
        //                                                                    s1.id));
        //                                                    j++;
        //                                                } else {
        //                                                    break;
        //                                                }
        //                                            }
        //                                        }
        //                                    }
        //                                }
        //                                return l;
        //
        //                            }
        //                        }).collectAsMap());
        //
        //        final Broadcast<HashMap<Integer, Integer>> qi = sc.broadcast(IDsMap);
        //
        //        JavaPairRDD<Integer, Tuple2<float[], Integer>> f = timeSeries.
        //                flatMapToPair(
        //                        new PairFlatMapFunction<Tuple2<Integer, float[]>, Integer, Tuple2<float[], Integer>>() {
        //
        //                            @Override
        //                            public Iterable<Tuple2<Integer, Tuple2<float[], Integer>>> call(
        //                                    Tuple2<Integer, float[]> t)
        //                            throws Exception {
        //                                HashMap<Integer, Integer> mp = qi.value();
        //
        //                                List<Tuple2<Integer, Tuple2<float[], Integer>>> l
        //                                                                                = new ArrayList<>();
        //
        //                                if(mp.containsKey(t._1)) {
        //                                    l.add(new Tuple2<>(t._1, new Tuple2<>(t._2,
        //                                                                          mp.
        //                                                                          get(t._1))));
        //                                }
        //
        //                                return l;
        //                            }
        //                        });
        //---------------------------------------------------------------------------------------------------------------------
        //q.destroy();
        //    public void ApproximateSearch(JavaRDD<float[]> QuerytimeSeries) {
        //
        //        JavaPairRDD<String, iSAXWord[]> q = QuerytimeSeries.mapToPair(
        //                new ParseQureyTimeSeries(
        //                        config.isNormalization(),
        //                        config.getWordLen())).groupByKey().mapValues(
        //                        new GroupQueryTimeSeries())
        //                .partitionBy(new HashPartitioner(config.getPartitions()));
        //
        //        JavaPairRDD<Integer, int[]> d = q.join(finalRDD).values().flatMapToPair(
        //                new PairFlatMapFunction<Tuple2<iSAXWord[], AbstractNode>, Integer, int[]>() {
        //
        //                    @Override
        //                    public Iterable<Tuple2<Integer, int[]>> call(
        //                            Tuple2<iSAXWord[], AbstractNode> t)
        //                    throws Exception {
        //                        List<Tuple2<Integer, int[]>> l
        //                                                             = new ArrayList<>();
        //                        for(iSAXWord col : t._1) {
        //
        //                            l.add(new Tuple2<>(col.id, t._2.
        //                                               ApproximateSearch(
        //                                                       col.saxWord)));
        //                        }
        //
        //                        return l;
        //                    }
        //                });
        //        JavaPairRDD<Integer, int[]> d = finalRDD.join(q).values().flatMapToPair(
        //                new PairFlatMapFunction<Tuple2<AbstractNode, iSAXWord[]>, Integer, int[]>() {
        //
        //                    @Override
        //                    public Iterable<Tuple2<Integer, int[]>> call(
        //                            Tuple2<AbstractNode, iSAXWord[]> t)
        //                    throws Exception {
        //                        List<Tuple2<Integer, int[]>> l
        //                                                             = new ArrayList<>();
        //                        for(iSAXWord col : t._2) {
        //
        //                            l.add(new Tuple2<>(col.id, t._1.
        //                                               ApproximateSearch(
        //                                                       col.saxWord)));
        //                        }
        //
        //                        return l;
        //
        //                    }
        //                });
        //        JavaPairRDD<Integer, int[]> d = q.leftOuterJoin(finalRDD).values().
        //                flatMapToPair(
        //                        new PairFlatMapFunction<Tuple2<iSAXWord[], Optional<AbstractNode>>, Integer, int[]>() {
        //
        //                            @Override
        //                            public Iterable<Tuple2<Integer, int[]>> call(
        //                                    Tuple2<iSAXWord[], Optional<AbstractNode>> t)
        //                            throws Exception {
        //                                List<Tuple2<Integer, int[]>> l
        //                                                                     = new ArrayList<>();
        //
        //                                if(t._2.isPresent()) {
        //                                    for(iSAXWord col : t._1) {
        //
        //                                        l.add(new Tuple2<>(col.id, t._2.get().
        //                                                           ApproximateSearch(
        //                                                                   col.saxWord)));
        //                                    }
        //                                } else {
        //                                    for(iSAXWord col : t._1) {
        //                                        l.add(new Tuple2<>(col.id, new int[1]));
        //                                    }
        //                                }
        //                                return l;
        //                            }
        //                        });
        //        JavaPairRDD<Integer, int[]> d = finalRDD.rightOuterJoin(q).values().
        //                flatMapToPair(
        //                        new PairFlatMapFunction<Tuple2<Optional<AbstractNode>, iSAXWord[]>, Integer, int[]>() {
        //
        //                            @Override
        //                            public Iterable<Tuple2<Integer, int[]>> call(
        //                                    Tuple2<Optional<AbstractNode>, iSAXWord[]> t)
        //                            throws
        //                            Exception {
        //
        //                                List<Tuple2<Integer, int[]>> l
        //                                                                     = new ArrayList<>();
        //
        //                                if(t._1.isPresent()) {
        //                                    for(iSAXWord col : t._2) {
        //
        //                                        l.add(new Tuple2<>(col.id, t._1.get().
        //                                                           ApproximateSearch(
        //                                                                   col.saxWord)));
        //                                    }
        //                                } else {
        //                                    for(iSAXWord col : t._2) {
        //                                        l.add(new Tuple2<>(col.id, new int[1]));
        //                                    }
        //                                }
        //                                return l;
        //                            }
        //                        });
        //List<Tuple2<Integer, Integer>> l = d.collect();
        //for(Tuple2<Integer, Integer> l1 : l) {
        //   System.out.println(l1._1 + " " + l1._2);
        // }
        // System.out.println(l.size());
        //f.saveAsTextFile("workspace/out");
        // System.out.println(f.toDebugString());
        return null;

    }


    //to change

    @Override
    public JavaPairRDD<Integer, ArrayList<Integer>> ApproximateSearch(JavaPairRDD<Integer, float[]> QueryTimeSeries, JavaSparkContext sc) {


        HashMap<Integer, iSAXWord[]> mp = new HashMap<>();

        mp.putAll(QueryTimeSeries.mapToPair(new ParseQureyTimeSeriesExtend(config.isNormalization(),config.getWordLen(),
                                                                     this.getPartitions())).groupByKey().mapValues
                (new GroupQueryTimeSeries()).collectAsMap());

        final Broadcast<HashMap<Integer, iSAXWord[]>> Query = sc.broadcast(mp);

        JavaPairRDD<Integer, ArrayList<Integer>> result = finalRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,
                AbstractNode>, Integer, Integer>() {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(Tuple2<Integer, AbstractNode> integerAbstractNodeTuple2) throws
                                                                                                              Exception {

                List<Tuple2<Integer, Integer>> l = new ArrayList<>();

                iSAXWord[] s = Query.value().get(integerAbstractNodeTuple2._1());

                if(s != null) {
                    for(iSAXWord s1 : s) {
                        int[] tempTab = integerAbstractNodeTuple2._2().ApproximateSearch(s1.saxWord);
                        if(tempTab != null) {

                            for(int aTempTab : tempTab) {
                                if(aTempTab != 0 ) {
                                    l.add(new Tuple2<>(s1.id, aTempTab));

                                }
                            }
                        }
                    }
                }
                return l;
            }
        }).groupByKey().mapValues(new Function<Iterable<Integer>, ArrayList< Integer>>() {

            @Override
            public ArrayList< Integer> call(Iterable<Integer> tt1) throws Exception {

                ArrayList< Integer> l = new ArrayList<>();

                Iterator<Integer> t  = tt1.iterator();

                while(t.hasNext()) {
                    l.add(t.next());

                }


                return l;
            }
        });















        return result;

    }

    @Override
    public void ExactSearch(JavaRDD<float[]> QuerytimeSeries) {

    }

    @Override
    public void saveAsTextFile(String path) {
        finalRDD.saveAsTextFile(path);

    }

    @Override
    public void saveAsObjectFile(String path) {
        finalRDD.saveAsObjectFile(path);

    }

    //    public List<Tuple2<String, AbstractNode>> collect() {
    //        return finalRDD.collect();
    //    }
    @Override
    public String toDebugString() {
        return finalRDD.toDebugString();
    }

    public static class getTimeSeriesFromString implements PairFunction<String, Integer, float[]> {

        private final Configuration config;

        public getTimeSeriesFromString(Configuration config) {
            this.config = config;
        }

        @Override
        public Tuple2<Integer, float[]> call(String v1) throws Exception {
            String[] TS = v1.split(",");
            if(TS.length >= (this.config.getTimeSeriesLength() + 1)) {
                float[] point = new float[this.config.getTimeSeriesLength()];
                for(int i = 1; i < point.length + 1; ++i) {
                    point[i - 1] = Float.parseFloat(TS[i]);
                }
                return new Tuple2<>(Integer.parseInt(TS[0]), point);
            } else {
                float[] point = new float[this.config.getTimeSeriesLength() + 1];
                for(int i = 1; i < TS.length + 1; ++i) {
                    point[i - 1] = Float.parseFloat(TS[i]);
                }

                for(int i = TS.length; i < point.length + 1; i++) {
                    point[i - 1] = 0;
                }
                return new Tuple2<>(Integer.parseInt(TS[0]), point);
            }

        }

    }


    @Override
    public void print() {
        List<String> p = finalRDD.map(new Function<Tuple2<Integer,AbstractNode>, String>() {
            @Override
            public String call(Tuple2<Integer, AbstractNode> v1) throws Exception {
                String str = "├── " + v1._1() +" Root \n";

                str += (v1._2().print("│   ",true));





                return str;
            }
        }).collect();


        for(String p1 : p) {
            System.out.print(p1);
        }
    }
}

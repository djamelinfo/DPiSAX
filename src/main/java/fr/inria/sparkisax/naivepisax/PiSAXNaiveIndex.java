/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.inria.sparkisax.naivepisax;


import fr.inria.sparkisax.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
public class PiSAXNaiveIndex extends iSAXindex {

    JavaRDD<RootNode> finalRDD;

    public PiSAXNaiveIndex(Configuration config) {
        super(config);
    }

    /**
     * @param timeSeriesRDD
     */
    @Override
    public void index(JavaPairRDD<Integer, float[]> timeSeriesRDD, int a , JavaSparkContext sc) {
        //JavaRDD<String> rdd1 = sc.textFile(File);

        this.timeSeries = timeSeriesRDD;/*
         * .persist(StorageLevel. MEMORY_AND_DISK_SER());
         */

        JavaPairRDD<short[], Integer> tsRDD = timeSeries.mapToPair(new ParseTimeSeries(config.isNormalization(), config.getWordLen())).persist(StorageLevel.MEMORY_AND_DISK());


        if(tsRDD.partitions().size() != config.getPartitions()) finalRDD.repartition(config.getPartitions());


        finalRDD = tsRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<short[], Integer>>, RootNode>() {

            @Override
            public Iterable<RootNode> call(Iterator<Tuple2<short[], Integer>> t) throws Exception {

                RootNode rootNode = new RootNode();
                while(t.hasNext()) {
                    Tuple2<short[], Integer> next = t.next();
                    rootNode.insert(next._1, next._2, config.getThreshold());
                }

                rootNode.dropTimeSeriesData();

                List<RootNode> l = new ArrayList<>();
                l.add(rootNode);
                return l;

            }
        });
    }

    public void getStat() {
        JavaPairRDD<Integer, String> rdd = finalRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<RootNode>, Integer, String>() {

            @Override
            public Iterable<Tuple2<Integer, String>> call(Iterator<RootNode> t) throws Exception {

                ArrayList<Tuple2<Integer, String>> liste = new ArrayList<>();
                int                                sum   = 0;
                int                                pro   = 0;
                int                                nbrT  = 0;
                while(t.hasNext()) {

                    RootNode b = t.next();
                    sum += b.getNbrOfNode();
                    int a = b.getPro();
                    if(pro < a) {
                        pro = a;
                    }
                    nbrT += b.getNbrTs();
                }

                liste.add(new Tuple2<>(pro,((int) (sum * 1.6)) + "," + nbrT));

                return liste;
            }
        });

        List<Tuple2<Integer, String>> p = rdd.collect();

        for(Tuple2<Integer, String> p1 : p) {
            System.out.println(p1._1 + "," + p1._2);
        }
    }

    @Override
    public JavaPairRDD<Integer, Tuple2<float[], Integer>> ApproximateSearch(JavaRDD<String> Query, JavaSparkContext sc) {





        //        JavaPairRDD<Integer, float[]> QuerytimeSeries = Query.mapToPair(
        //                new getTimeSeriesFromString(config));
        //
        //        List<iSAXWord> mp = QuerytimeSeries.map(new ParseQureyTimeSeries(
        //                config.isNormalization(),
        //                config.getWordLen())).collect();
        //
        //        final Broadcast<List<iSAXWord>> q = sc.broadcast(mp);
        //
        //        HashMap<Integer, Integer> IDsMap = new HashMap<>();
        //
        //        IDsMap.putAll(finalRDD.flatMapToPair(
        //                new PairFlatMapFunction<RootNode, Integer, Integer>() {
        //
        //                    @Override
        //                    public Iterable<Tuple2<Integer, Integer>> call(
        //                            RootNode t)
        //                    throws
        //                    Exception {
        //                        List<Tuple2<Integer, Integer>> l = new ArrayList<>();
        //
        //                        List<iSAXWord> Querys = q.value();
        //
        //                        for(iSAXWord Query : Querys) {
        //
        //                            int[] tempTab = t.ApproximateSearch(Query.saxWord);
        //
        //                            if(tempTab != null) {
        //                                int j = 0;
        //                                for(int i = 0; i < tempTab.length; i++) {
        //
        //                                    if(tempTab[i] != 0 && j < 10) {
        //                                        l.
        //                                        add(new Tuple2<>(tempTab[i], Query.id));
        //                                        j++;
        //                                    } else {
        //                                        break;
        //                                    }
        //                                }
        //                            }
        //                        }
        //
        //                        return l;
        //
        //                    }
        //                }).collectAsMap());
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
        //
        //        System.out.println(f.toDebugString());
        return null;

    }





    @Override
    public JavaPairRDD<Integer, ArrayList<Integer>> ApproximateSearch(JavaPairRDD<Integer, float[]> QueryTimeSeries,JavaSparkContext sc) {

        HashMap<String, iSAXWord> mp = new HashMap<>();


        mp.putAll(QueryTimeSeries.mapToPair(new ParseQureyTimeSeries(config.isNormalization(), config.getWordLen())).collectAsMap());
        final Broadcast<HashMap<String, iSAXWord>> Query = sc.broadcast(mp);

        JavaPairRDD<Integer, ArrayList<Integer>> result = finalRDD.flatMapToPair(new PairFlatMapFunction<RootNode,
                Integer,
                Integer>
                () {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(RootNode rootNode) throws Exception {

                List<Tuple2<Integer, Integer>> l = new ArrayList<>();
                HashMap<String, iSAXWord> mp = Query.getValue();

                for(Map.Entry<String, iSAXWord> entrySet : mp.entrySet()) {

                    int[] tempTab = rootNode.ApproximateSearch(entrySet.getValue().saxWord);
                    if(tempTab != null) {
                        int j = 0;
                        for(int aTempTab : tempTab) {
                            if(aTempTab != 0 ) {
                                l.add(new Tuple2<>(entrySet.getValue().id, aTempTab));

                            }
                        }
                    }


                }


                return l;
            }
        }).groupByKey().mapValues(new Function<Iterable<Integer>, ArrayList<Integer>>() {
            @Override
            public ArrayList<Integer> call(Iterable<Integer> v1) throws Exception {

                ArrayList< Integer> l = new ArrayList<>();

                Iterator<Integer> t  = v1.iterator();

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
            //to do
    }

    @Override
    public void saveAsTextFile(String path) {
        finalRDD.saveAsTextFile(path);

    }

    @Override
    public void saveAsObjectFile(String path) {
        finalRDD.saveAsObjectFile(path);

    }

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

}

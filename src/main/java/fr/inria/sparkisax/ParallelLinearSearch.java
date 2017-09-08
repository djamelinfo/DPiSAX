package fr.inria.sparkisax;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by djamel on 23/11/16.
 */
public class ParallelLinearSearch implements Serializable, Cloneable {


    public static JavaPairRDD<Integer, ArrayList<Integer>> parallelLinearSearch(JavaPairRDD<Integer, float[]> TimeSeriesRDD, JavaPairRDD<Integer, float[]> QueryRDD, JavaSparkContext sc, final int K) {


        List<Tuple2<Integer, float[]>> QueryList = QueryRDD.collect();


        final Broadcast<List<Tuple2<Integer, float[]>>> QueryListBroadcast = sc.broadcast(QueryList);


        JavaPairRDD<Integer, Tuple2<Integer, Float>> rdd = TimeSeriesRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, float[]>, Integer, Tuple2<Integer, Float>>() {

            public Iterable<Tuple2<Integer, Tuple2<Integer, Float>>> call(Tuple2<Integer, float[]> integerTuple2) throws Exception {

                List<Tuple2<Integer, float[]>>                QueryList = QueryListBroadcast.getValue();
                List<Tuple2<Integer, Tuple2<Integer, Float>>> outList   = new ArrayList<Tuple2<Integer, Tuple2<Integer, Float>>>();

                for(Tuple2<Integer, float[]> query : QueryList) {
                    outList.add(new Tuple2<Integer, Tuple2<Integer, Float>>(query._1(), new Tuple2<Integer, Float>(integerTuple2._1(), SAXutils.EuclideanDistance(query._2(), integerTuple2._2(), 0, query._2().length))));
                }
                return outList;
            }
        });


        return rdd.combineByKey(new Function<Tuple2<Integer, Float>, ArrayList<Tuple2<Integer, Float>>>() {
            @Override
            public ArrayList<Tuple2<Integer, Float>> call(Tuple2<Integer, Float> v1) throws Exception {
                ArrayList<Tuple2<Integer, Float>> list = new ArrayList<Tuple2<Integer, Float>>();

                list.add(v1);

                return list;
            }
        }, new Function2<ArrayList<Tuple2<Integer, Float>>, Tuple2<Integer, Float>, ArrayList<Tuple2<Integer, Float>>>() {
            @Override
            public ArrayList<Tuple2<Integer, Float>> call(ArrayList<Tuple2<Integer, Float>> v1, Tuple2<Integer, Float> v2) throws Exception {

                ArrayList<Tuple2<Integer, Float>> list = new ArrayList<Tuple2<Integer, Float>>();

                if(v1.size() >= K) {
                    TreeMap<Float, Integer> treeMap = new TreeMap<Float, Integer>();

                    for(Tuple2<Integer, Float> tup : v1) {
                        treeMap.put(tup._2(), tup._1());
                    }
                    treeMap.put(v2._2(), v2._1());

                    for(Map.Entry<Float, Integer> ent : treeMap.entrySet()) {
                        if(list.size() < K) {
                            list.add(new Tuple2<Integer, Float>(ent.getValue(), ent.getKey()));

                        } else {
                            break;
                        }
                    }


                } else {
                    list.add(v2);
                    list.addAll(v1);
                }


                return list;
            }
        }, new Function2<ArrayList<Tuple2<Integer, Float>>, ArrayList<Tuple2<Integer, Float>>, ArrayList<Tuple2<Integer, Float>>>() {
            @Override
            public ArrayList<Tuple2<Integer, Float>> call(ArrayList<Tuple2<Integer, Float>> v1, ArrayList<Tuple2<Integer, Float>> v2) throws Exception {

                ArrayList<Tuple2<Integer, Float>> list = new ArrayList<Tuple2<Integer, Float>>();

                if((v1.size() + v2.size()) >= K) {

                    TreeMap<Float, Integer> treeMap = new TreeMap<Float, Integer>();

                    for(Tuple2<Integer, Float> tup : v1) {
                        treeMap.put(tup._2(), tup._1());
                    }

                    for(Tuple2<Integer, Float> tup : v2) {
                        treeMap.put(tup._2(), tup._1());
                    }


                    for(Map.Entry<Float, Integer> ent : treeMap.entrySet()) {
                        if(list.size() < K) {
                            list.add(new Tuple2<Integer, Float>(ent.getValue(), ent.getKey()));

                        } else {
                            break;
                        }
                    }

                } else {
                    list.addAll(v1);
                    list.addAll(v2);
                }

                return list;
            }
        }).mapValues(new Function<ArrayList<Tuple2<Integer, Float>>, ArrayList<Integer>>() {
            @Override
            public ArrayList<Integer> call(ArrayList<Tuple2<Integer, Float>> v1) throws Exception {
                ArrayList<Integer> list = new ArrayList<Integer>();

                for(Tuple2<Integer, Float> tup : v1) {
                    list.add(tup._1());
                }
                return list;
            }
        });





    }
}

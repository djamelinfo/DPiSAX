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

import fr.inria.randomWalk.RandomWalk;
import fr.inria.sparkisax.naivepisax.PiSAXNaiveIndex;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Double;
import scala.Float;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class PiSAX {

    public static Configuration config;


    public static <T> List<T> intersection(List<T> list1, List<T> list2) {
        List<T> list = new ArrayList<T>();

        for(T t : list1) {
            if(list2.contains(t)) {
                list.add(t);
            }
        }

        return list;
    }

    public static void main(String[] args) throws Exception {

        String  output               = "";
        String  ListOfinputQueryFile = "";
        String  configFile           = "";
        String  input                = "";
        String  p                    = "";
        String  configVal            = "";
        String  A                    = "";
        long    numbTS               = 0;
        long    numbOfQuery          = 0;
        int     timeSeriesLength     = 0;
        Boolean test                 = false;
        Boolean log                  = false;
        Boolean ls                   = false;

        int a = 1;

        if(args.length < 1) {
            System.err.
                              println("Usage: PDiSAX [OPTION]...");
            System.out.println("Try 'PDiSAX -h' for more information.");
            System.exit(1);
        }

        for(int i = 0; i < args.length; i++) {
            System.out.println(i + " " + args[i]);
            switch(args[i]) {

                case "-f":
                    i++;
                    input = args[i];
                    break;
                case "-p":
                    i++;
                    p = args[i];
                    break;
                case "-t":
                    test = true;
                    break;
                case "-g":
                    i++;
                    numbTS = Long.parseLong(args[i]);
                    i++;
                    timeSeriesLength = Integer.parseInt(args[i]);
                    break;
                case "-G":
                    i++;
                    numbOfQuery = Integer.parseInt(args[i]);
                    break;
                case "-o":
                    i++;
                    output = args[i];
                    break;
                case "-q":
                    i++;
                    ListOfinputQueryFile = args[i];
                    break;
                case "-c":
                    i++;
                    configFile = args[i];
                    break;
                case "--config":
                    i++;
                    configVal = args[i];
                    break;
                case "-a":
                    i++;
                    a = Integer.parseInt(args[i]);
                    break;
                case "-A":
                    i++;
                    A = args[i];
                    break;
                case "--log":

                    log = true;
                    break;

                case "--ls":

                    ls = true;
                    break;

                case "-h":
                    System.out.println("Usage: PDiSAX [OPTION]...\n");
                    System.out.println("Options:");
                    System.out.println("  -f\t\t  Input file");
                    System.out.println("  -g\t\t  RandomWalk Time Series Generator, you must give number of Time Series and Number of elements in each Time Series.");
                    System.out.println("  -G\t\t  RandomWalk Query Time Series Generator, you must give number of Time Series and .");
                    System.out.println("  -o\t\t  Output directory.");
                    System.out.println("  -q\t\t  Comma-separated list of input query file.");
                    System.out.println("  -c\t\t  Path to a file from which to load extra properties. If not specified, this will use defaults propertie.");
                    System.out.println("  --config\t  Change given configuration variables, NAME=VALUE (e.g fraction, " + "partitions, " + "normalization, " + "wordLen, " + "timeSeriesLength, " + "threshold, " + "k)");
                    System.out.println("  -a\t\t  Use 1 for DPiSAX, use 3 for DbasicPiSAX");
                    System.out.println("  -A\t\t  Use 1 for (DPiSAX and DbasicPiSAX), use 2 for DiSAX");
                    System.out.println("  --ls\t\t  Parallel Linear Search");
                    System.out.println("  -p\t\t  Preprocessing. you must give input file.");
                    System.out.println("  -h\t\t  Show this help message and exit.  \n");

                    System.out.println("Full documentation at: <http://djameledine-yagoubi.info/projects/DPiSAX/>");
                    System.exit(1);
                    break;
                default:
                    System.err.println("PDiSAX invalid option -- " + args[i].replace("-", ""));
                    System.out.println("Try 'PDiSAX -h' for more information.");
                    System.exit(1);
                    break;

            }

        }

        Logger logger = LogManager.getRootLogger();

        if(!log) {
            logger.setLevel(Level.ERROR);
        }


        SparkConf conf = new SparkConf().setAppName("PDiSAX :" + Arrays.toString(args)).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
                set("spark.kryo.registrationRequired", "true");

        //@formatter:off
        conf.registerKryoClasses(new Class<?>[]{
                Class.forName("fr.inria.sparkisax.AbstractNode"),
                Class.forName("fr.inria.sparkisax.Configuration"),
                Class.forName("fr.inria.sparkisax.CustomComparator"),
                Class.forName("fr.inria.sparkisax.CustomPartitioner"),
                Class.forName("fr.inria.sparkisax.GroupQueryTimeSeries"),
                Class.forName("fr.inria.sparkisax.Index"),
                Class.forName("fr.inria.sparkisax.InternalNode"),
                Class.forName("fr.inria.sparkisax.TerminalNode"),
                Class.forName("fr.inria.sparkisax.PartitionsUtil"),
                Class.forName("fr.inria.sparkisax.PiSAX"),
                Class.forName("fr.inria.sparkisax.PiSAXindex"),
                Class.forName("fr.inria.sparkisax.SAXutils"),
                Class.forName("fr.inria.sparkisax.iSAXSplit"),
                Class.forName("fr.inria.sparkisax.iSAXWord"),
                Class.forName("fr.inria.sparkisax.ParseQureyTimeSeries"),
                Class.forName("fr.inria.sparkisax.ParseTimeSeries"),
                Class.forName("fr.inria.sparkisax.iSAXPartition"),
                Class.forName("java.util.ArrayList"),
                Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
                Class.forName("scala.collection.convert.Wrappers$"),
                scala.Tuple3[].class,
                float[].class,
                short[].class,
                int[].class,
                Object[].class,
                iSAXWord[].class,
                scala.Tuple2[].class,
                short[][].class,
                byte[].class,
                org.apache.spark.api.java.JavaUtils.SerializableMapWrapper.class,
                java.util.HashMap.class,
                Class.forName("scala.reflect.ClassTag$$anon$1"),
                java.lang.Class.class,
                String[].class
                //
                //Class.forName("fr.inria.sparkisax.PiSAX.getTimeSeriesFromString")
        });

        //@formatter:on
        JavaSparkContext sc = new JavaSparkContext(conf);


        if(configFile.equals("")) {

            config = new Configuration();

        } else {

            config = new Configuration(sc.textFile(configFile).collect());

        }

        if(!configVal.equals("")) {
            config.changeConfig(configVal);
        }

        config.printConfig();


        if(test) {



            //test the accuracy of DPiSAX and DiSAX

            PiSAXindex      iSAX1 = new PiSAXindex(config);
            PiSAXNaiveIndex iSAX2 = new PiSAXNaiveIndex(config);

            JavaPairRDD<Integer, float[]> rdd1 = RandomWalk.GeneratedRandomWalkData(sc, numbTS, timeSeriesLength, config.getPartitions()).persist(StorageLevel.MEMORY_AND_DISK());
            JavaPairRDD<Integer, float[]> QueryRdd = RandomWalk.GeneratedRandomWalkData(sc, numbOfQuery,timeSeriesLength, config.getPartitions()).persist(StorageLevel.MEMORY_AND_DISK());


            //JavaPairRDD<Integer, float[]> QueryRdd = rdd1.sample(false, ((double) numbOfQuery) / ((double) numbTS)).persist(StorageLevel.MEMORY_AND_DISK());

            iSAX1.index(rdd1, a, sc);
            iSAX2.index(rdd1, a, sc);

            JavaPairRDD<Integer, ArrayList<Integer>> q1 = iSAX1.ApproximateSearch(QueryRdd, sc).repartition(1).persist(StorageLevel.MEMORY_AND_DISK());
            JavaPairRDD<Integer, ArrayList<Integer>> q2 = iSAX2.ApproximateSearch(QueryRdd, sc).repartition(1).persist(StorageLevel.MEMORY_AND_DISK());
            JavaPairRDD<Integer, ArrayList<Integer>> q3 = ParallelLinearSearch.parallelLinearSearch(rdd1, QueryRdd, sc, config.getK()).repartition(1).persist(StorageLevel.MEMORY_AND_DISK());

            JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = q1.join(q3).mapToPair(new PairFunction<Tuple2<Integer, Tuple2<ArrayList<Integer>, ArrayList<Integer>>>, Integer, Integer>() {
                @Override
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<ArrayList<Integer>, ArrayList<Integer>>> integerTuple2Tuple2) throws Exception {

                    int i = 0;
                    for(Integer ts : integerTuple2Tuple2._2._2()) {
                        if(integerTuple2Tuple2._2()._1().contains(ts)) {
                            i++;
                        }
                    }
                    return new Tuple2<Integer, Integer>(integerTuple2Tuple2._1(), i);
                }
            });


            JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD2 = q2.join(q3).mapToPair(new PairFunction<Tuple2<Integer, Tuple2<ArrayList<Integer>, ArrayList<Integer>>>, Integer, Integer>() {
                @Override
                public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<ArrayList<Integer>, ArrayList<Integer>>> integerTuple2Tuple2) throws Exception {
                    int i = 0;
                    for(Integer ts : integerTuple2Tuple2._2._2()) {
                        if(integerTuple2Tuple2._2()._1().contains(ts)) {
                            i++;
                        }
                    }
                    return new Tuple2<Integer, Integer>(integerTuple2Tuple2._1(), i);
                }
            });


            //q1.saveAsTextFile("q3" + Math.random());
            // q2.saveAsTextFile("q3" + Math.random());


            //q3.repartition(1).saveAsTextFile("q3");


            List<Tuple2<Integer, Integer>> l1 = integerIntegerJavaPairRDD.repartition(1).sortByKey().collect();
            List<Tuple2<Integer, Integer>> l2 = integerIntegerJavaPairRDD2.repartition(1).sortByKey().collect();


            double P1 = 0;
            double P2 = 0;


            for(int i = 0; i < numbOfQuery; i++) {
                String str = "";

                if(i < l1.size()) {
                    P1 += l1.get(i)._2();
                    str += l1.get(i);
                } else {
                    str += "\t";
                }

                if(i < l2.size()) {
                    str += "\t" + l2.get(i);
                    P2 += l2.get(i)._2();
                }


                logger.error(str);


            }


            logger.error("DPiSAX Accuracy = " + ((P1 / numbOfQuery) * 100) / ((double) config.getK()));
            logger.error("DiSAX Accuracy = " + ((P2 / numbOfQuery) * 100) / ((double) config.getK()));


            logger.error("q1 give this = " + q1.count() + " q2 give this = " + q2.count());


        } else {

            if (!p.equals("")) {

                JavaPairRDD<Integer, float[]> rdd1 = sc.textFile(p).flatMap(new FlatMapFunction<String, float[]>() {
                    @Override
                    public Iterable<float[]> call(String s) throws Exception {

                        List<float[]> l = new ArrayList<>();

                        String[] t = s.split(",");



                        int j =0;
                        for (int i = 0; i < (t.length/256); i++) {
                            float[] tmp = new float[256];
                            for (int k = 0; k < 256; k++) {
                                tmp[k] = java.lang.Float.valueOf(t[j]);
                                j++;
                            }
                            l.add(tmp);

                        }





                        return l;
                    }
                }).zipWithUniqueId().mapToPair(new PairFunction<Tuple2<float[], Long>, Integer, float[]>() {
                    @Override
                    public Tuple2<Integer, float[]> call(Tuple2<float[], Long> longTuple2) throws Exception {
                        return new Tuple2<>(longTuple2._2.intValue(), longTuple2._1);
                    }
                });

                rdd1.saveAsObjectFile("objectFile");


            }else{


                if (ls) {

                    //this section is for parallel linear search

                    JavaPairRDD<Integer, float[]> rdd1 = RandomWalk.GeneratedRandomWalkData(sc, numbTS, timeSeriesLength, config.getPartitions());
                    JavaPairRDD<Integer, float[]> QueryRdd = RandomWalk.GeneratedRandomWalkData(sc, numbOfQuery, timeSeriesLength, config.getPartitions());

                    JavaPairRDD<Integer, ArrayList<Integer>> q3 = ParallelLinearSearch.parallelLinearSearch(rdd1, QueryRdd, sc, config.getK()).repartition(1);

                    for (Tuple2<Integer, ArrayList<Integer>> t : q3.collect()) {
                        logger.error("(" + t._1() + "," + t._2().size() + ")");
                    }


                } else {

                    iSAXindex iSAX;

                    switch (A) {
                        case "2":
                            iSAX = new PiSAXNaiveIndex(config);
                            break;
                        default:
                            iSAX = new PiSAXindex(config);
                            break;

                    }

                    JavaPairRDD<Integer, float[]> rdd1;

                    if (!input.equals("")) {
                        rdd1 = sc.objectFile(input).mapToPair(new PairFunction<Object, Integer, float[]>() {
                            @Override
                            public Tuple2<Integer, float[]> call(Object t) throws Exception {
                                return (Tuple2<Integer, float[]>) t;
                            }
                        });
                    } else {
                        rdd1 = RandomWalk.GeneratedRandomWalkData(sc, numbTS, timeSeriesLength, config.getPartitions());

                    }

                    iSAX.index(rdd1, a, sc);

                    switch (a) {
                        case 1:
                            iSAX.getStat();
                            break;
                        default:
                            iSAX.getStat2();
                            break;
                    }


                    //iSAX.printIndexRoots();


                    if (!log) {
                        iSAX.print();
                    }


                    if (!output.equals("")) {
                        iSAX.saveAsObjectFile(output + "-index");
                        rdd1.saveAsObjectFile(output + "-RawData");

                        if (!ListOfinputQueryFile.equals("")) {

                            String[] QueryFile = ListOfinputQueryFile.split(",");
                            for (String QueryFile1 : QueryFile) {
                                System.out.println(QueryFile1);
                            }

                            for (String QueryFile1 : QueryFile) {
                                JavaRDD<String> QuerytimeSeries = sc.textFile(QueryFile1);
                                iSAX.ApproximateSearch(QuerytimeSeries, sc).repartition(1).sortByKey().saveAsTextFile(output + "-" + QueryFile1);
                            }
                        }
                    }

                    if (numbOfQuery > 0) {
                        JavaPairRDD<Integer, float[]> QueryRdd;
                        for (int i = 1; i <= 2; i++) {
                            QueryRdd = RandomWalk.GeneratedRandomWalkData(sc, numbOfQuery * i, timeSeriesLength, config.getPartitions());
                            iSAX.ApproximateSearch(QueryRdd, sc).repartition(1).sortByKey().saveAsTextFile(numbOfQuery * i + "" + Math.random() * 100000);
                        }
                    }

                    System.out.println(iSAX.toDebugString());

                }
        }
        }

        sc.stop();

    }

}

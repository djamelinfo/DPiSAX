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

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class Index implements FlatMapFunction<Iterator<Tuple2<short[], Integer>>, AbstractNode> {


    private Configuration config;
    public Index(Configuration config) {
        this.config = config;
    }


    public Iterable<AbstractNode> call(Iterator<Tuple2<short[], Integer>> t) throws Exception {
        Map<String, AbstractNode> childs = new Hashtable<>();

        while(t.hasNext()) {

            Tuple2<short[], Integer> next     = t.next();
            String                   hashcode = "";
            for(int i = 0; i < next._1.length; i++) {
                hashcode += (next._1[i] >>> 8) + ";";
            }
            if(childs.containsKey(hashcode)) {
                if(childs.get(hashcode).getType()) {
                    childs.get(hashcode).insert(next._1, next._2, config.getThreshold());
                } else {
                    if(childs.get(hashcode).getNbrTs() == config.getThreshold() && ((TerminalNode) childs.get(hashcode)).canBeSplit()) {

                        short[][] saxWordsLocal      = ((TerminalNode) childs.get(hashcode)).getSaxWords();
                        int[]     timeSeriesIDsLocal = ((TerminalNode) childs.get(hashcode)).getTimeSeriesIDs();
                        short[]   saxWordLocal       = childs.get(hashcode).getSaxWord();
                        byte[]    saxCardLocal1      = childs.get(hashcode).getSaxCard();
                        byte[]    saxCardLocal2      = childs.get(hashcode).getSaxCard().clone();

                        childs.remove(hashcode);

                        int newSplitSymbol = iSAXSplit.getSplitsSymbol(saxWordsLocal, saxCardLocal2);

                        if(newSplitSymbol != -1) {
                            saxCardLocal2[newSplitSymbol] -= 1;
                            short[] saxWordNode1 = new short[next._1.length];

                            for(int i = 0; i < next._1.length; i++) {
                                saxWordNode1[i] = (short) (saxWordsLocal[0][i] >>> saxCardLocal2[i]);
                            }

                            AbstractNode tempNode1 = new TerminalNode(saxWordNode1, saxCardLocal2);

                            short[] saxWordNode2 = saxWordNode1.clone();

                            char[] ch = Integer.toBinaryString(saxWordNode2[newSplitSymbol]).toCharArray();

                            if(ch[ch.length - 1] == '0') {
                                saxWordNode2[newSplitSymbol] += 1;
                            } else {
                                saxWordNode2[newSplitSymbol] -= 1;
                            }

                            AbstractNode tempNode2 = new TerminalNode(saxWordNode2, saxCardLocal2);


                            childs.put(hashcode, new InternalNode(tempNode1, tempNode2, saxWordLocal, saxCardLocal1));

                            for(int i = 0; i < saxWordsLocal.length; i++) {
                                childs.get(hashcode).insert(saxWordsLocal[i], timeSeriesIDsLocal[i], config.getThreshold());
                            }

                            childs.get(hashcode).insert(next._1, next._2, config.getThreshold());

                        } else {
                            childs.put(hashcode, new TerminalNode(saxWordsLocal, timeSeriesIDsLocal, saxWordLocal, saxCardLocal1));
                            childs.get(hashcode).insert(next._1, next._2, config.getThreshold());
                        }

                    } else {
                        childs.get(hashcode).insert(next._1, next._2, config.getThreshold());
                    }

                }
            } else {

                byte[]  saxCardTemp = new byte[next._1.length];
                short[] saxWord     = new short[next._1.length];

                Arrays.fill(saxCardTemp, (byte) 8);

                for(int i = 0; i < next._1.length; i++) {
                    saxWord[i] = (short) (next._1[i] >>> saxCardTemp[i]);
                }

                AbstractNode tempNode = new TerminalNode(saxWord, saxCardTemp);
                tempNode.insert(next._1, next._2, config.
                                                                getThreshold());

                childs.put(hashcode, tempNode);

            }
        }

        return childs.values();

    }

}

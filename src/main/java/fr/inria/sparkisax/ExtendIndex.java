package fr.inria.sparkisax;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by djamel on 21/11/16.
 */
public class ExtendIndex implements PairFlatMapFunction<Iterator<Tuple2<String, iSAXWord>>,Integer,  AbstractNode> {


    private Configuration              config;
    private Map<String, iSAXPartition> noOfPartitioners;

    public ExtendIndex(Configuration config, Map<String, iSAXPartition> noOfPartitioners) {
        this.config = config;
        this.noOfPartitioners = noOfPartitioners;
    }

    @Override
    public Iterable<Tuple2<Integer,AbstractNode>> call(Iterator<Tuple2<String, iSAXWord>> tuple2Iterator) throws
                                                                                                       Exception {

        ArrayList<Tuple2<Integer,AbstractNode>> childs = new ArrayList<>();
        AbstractNode            node   = null;
        int partitionId = 0;


        if(tuple2Iterator.hasNext()) {


            Tuple2<String, iSAXWord> first = tuple2Iterator.next();

            iSAXPartition sax = noOfPartitioners.get(first._1());

            partitionId = sax.getPartitionID();

            byte[]  card    = sax.getSaxCard().clone();
            short[] saxWord = new short[card.length];

            for(int i = 0; i < card.length; i++) {
                saxWord[i] = (short) (first._2().saxWord[i] >>> card[i]);
            }


            node = new TerminalNode(saxWord, card);

            node.insert(first._2.saxWord, first._2().id, config.getThreshold());

        }
        while(tuple2Iterator.hasNext()) {

            Tuple2<String, iSAXWord> first = tuple2Iterator.next();

            if(node.getType()) {
                node.insert(first._2.saxWord, first._2().id, config.getThreshold());
            } else {

                if(node.getNbrTs() >= config.getThreshold() && ((TerminalNode) node).canBeSplit()) {


                    short[][] saxWordsLocal      = ((TerminalNode) node).getSaxWords();
                    int[]     timeSeriesIDsLocal = ((TerminalNode) node).getTimeSeriesIDs();
                    short[]   saxWordLocal       = node.getSaxWord();
                    byte[]    saxCardLocal1      = node.getSaxCard();
                    byte[]    saxCardLocal2      = node.getSaxCard().clone();

                    int newSplitSymbol = PartitionsUtil.getSymbolForIndexing(saxWordsLocal, saxCardLocal2);

                    if(newSplitSymbol != -1) {
                        saxCardLocal2[newSplitSymbol] -= 1;
                        short[] saxWordNode1 = new short[first._2().saxWord.length];

                        for(int i = 0; i < saxCardLocal1.length; i++) {
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


                        node = new InternalNode(tempNode1, tempNode2, saxWordLocal, saxCardLocal1);

                        for(int i = 0; i < saxWordsLocal.length; i++) {
                            node.insert(saxWordsLocal[i], timeSeriesIDsLocal[i], config.getThreshold());
                        }

                        node.insert(first._2.saxWord, first._2.id, config.getThreshold());

                    } else {
                        node.insert(first._2.saxWord, first._2.id, config.getThreshold());
                    }


                } else {
                    node.insert(first._2.saxWord, first._2().id, config.getThreshold());
                }

            }


        }


        childs.add(new Tuple2<Integer, AbstractNode>(partitionId,node));
        return childs;
    }
}

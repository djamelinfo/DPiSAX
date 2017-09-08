/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.inria.sparkisax.naivepisax;

import fr.inria.sparkisax.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class RootNode implements Serializable, Cloneable {

    private Map<String, AbstractNode> childs = new HashMap<>();

    public void insert(final short[] saxWordOfTs, final int id, final short threshold) throws Exception {

        String hashcode = "";
        for(short saxWordOfT : saxWordOfTs) {
            hashcode += (saxWordOfT >>> 8) + ";";
        }

        if(childs.containsKey(hashcode)) {
            if(childs.get(hashcode).getType()) {
                childs.get(hashcode).insert(saxWordOfTs, id, threshold);
            } else {
                if(childs.get(hashcode).getNbrTs() == threshold && ((TerminalNode) childs.get(hashcode)).canBeSplit()) {

                    short[][] saxWordsLocal      = ((TerminalNode) childs.get(hashcode)).getSaxWords();
                    int[]     timeSeriesIDsLocal = ((TerminalNode) childs.get(hashcode)).getTimeSeriesIDs();
                    short[]   saxWordLocal       = childs.get(hashcode).getSaxWord();
                    byte[]    saxCardLocal1      = childs.get(hashcode).getSaxCard();
                    byte[] saxCardLocal2 = childs.get(hashcode).getSaxCard().
                            clone();

                    childs.remove(hashcode);

                    int newSplitSymbol = iSAXSplit.
                                                          getSplitsSymbol(saxWordsLocal, saxCardLocal2);

                    if(newSplitSymbol != -1) {
                        saxCardLocal2[newSplitSymbol] -= 1;
                        short[] saxWordNode1 = new short[saxWordOfTs.length];

                        for(int i = 0; i < saxWordOfTs.length; i++) {
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

                        //                            for(int i = 1; i < saxWordsLocal.length; i++) {
                        //                                if((saxWordsLocal[i][newSplitSymbol]
                        //                                        >>> saxCardLocal2[newSplitSymbol])
                        //                                        != (saxWordsLocal[0][newSplitSymbol]
                        //                                        >>> saxCardLocal2[newSplitSymbol])) {
                        //                                    tempNode2 = new TerminalNode(
                        //                                            saxWordsLocal[i],
                        //                                            saxCardLocal2);
                        //                                    break;
                        //                                }
                        //                            }
                        //
                        //                            if(tempNode2 == null) {
                        //
                        //                                short[] sax = saxWordsLocal[0].clone();
                        //
                        //                                sax[newSplitSymbol]
                        //                                        = (short) ((sax[newSplitSymbol]
                        //                                        >>> saxCardLocal2[newSplitSymbol]) + 1);
                        //
                        //                                tempNode2 = new TerminalNode(sax,
                        //                                                             saxCardLocal2);
                        //
                        //                            }
                        childs.put(hashcode, new InternalNode(tempNode1, tempNode2, saxWordLocal, saxCardLocal1));

                        for(int i = 0; i < saxWordsLocal.length; i++) {
                            childs.get(hashcode).
                                    insert(saxWordsLocal[i], timeSeriesIDsLocal[i], threshold);
                        }

                        childs.get(hashcode).insert(saxWordOfTs, id, threshold);

                    } else {
                        childs.put(hashcode, new TerminalNode(saxWordsLocal, timeSeriesIDsLocal, saxWordLocal, saxCardLocal1));
                        childs.get(hashcode).insert(saxWordOfTs, id, threshold);
                    }

                } else {
                    childs.get(hashcode).insert(saxWordOfTs, id, threshold);
                }

            }
        } else {

            byte[]  saxCardTemp = new byte[saxWordOfTs.length];
            short[] saxWord     = new short[saxWordOfTs.length];

            Arrays.fill(saxCardTemp, (byte) 8);

            for(int i = 0; i < saxWordOfTs.length; i++) {
                saxWord[i] = (short) (saxWordOfTs[i] >>> saxCardTemp[i]);
            }

            AbstractNode tempNode = new TerminalNode(saxWord, saxCardTemp);
            tempNode.insert(saxWordOfTs, id, threshold);

            childs.put(hashcode, tempNode);

        }

    }

    public void dropTimeSeriesData() {
        for(Map.Entry<String, AbstractNode> entrySet : childs.entrySet()) {

            entrySet.getValue().dropTimeSeriesData();

        }
    }

    public int[] ApproximateSearch(final short[] saxWordOfTs) throws Exception {
        String hashcode = "";
        for(short saxWordOfT : saxWordOfTs) {
            hashcode += (saxWordOfT >>> 8) + ";";
        }

        if(childs.containsKey(hashcode)) {
            return childs.get(hashcode).ApproximateSearch(saxWordOfTs);
        } else {

            // need more detal

            return new int[1];
        }

    }


    public int getNbrOfNode() {
        int sum = 0;

        for(Map.Entry<String, AbstractNode> entrySet : childs.entrySet()) {
            sum += entrySet.getValue().getNbrOfNode();
        }

        return sum;
    }

    public int getPro() {
        int pro = 0;

        for(Map.Entry<String, AbstractNode> entrySet : childs.entrySet()) {
            int a = entrySet.getValue().getPro();

            if(pro < a) {
                pro = a;
            }
        }

        return pro;
    }


    public int getNbrTs() {
        int sum = 0;

        for(Map.Entry<String, AbstractNode> entrySet : childs.entrySet()) {
            sum += entrySet.getValue().getNbrTs();
        }

        return sum;
    }

    public String print(){
        return print("", true);

    }


    private String print(String prefix, boolean isTail){

        String str = prefix + (isTail ? "└── " : "├── ") + "Root\n";

        int i = 0;

        AbstractNode n = null;

        for(Map.Entry<String, AbstractNode> node:childs.entrySet()) {

            if(i < childs.size() - 1) {

                str += node.getValue().print(prefix + (isTail ? "    " : "│   "), false);
                i++;

            }else{
                n = node.getValue();
                break;

            }


        }

        if(childs.size() > 0)
            str += n.print(prefix + (isTail ?"    " : "│   "), true);



        return str;



    }




}

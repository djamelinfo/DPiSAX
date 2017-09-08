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

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class InternalNode extends AbstractNode {

    public AbstractNode node1;
    public AbstractNode node2;

    /**
     * @param saxWord
     * @param saxCard
     * @param nodeType
     */
    public InternalNode(short[] saxWord, byte[] saxCard, boolean nodeType) {
        super(saxWord, saxCard, true);
    }

    /**
     * @param node1
     * @param node2
     * @param saxWord
     * @param saxCard
     */
    public InternalNode(AbstractNode node1, AbstractNode node2, short[] saxWord, byte[] saxCard) {
        super(saxWord, saxCard, true);
        this.node1 = node1;
        this.node2 = node2;
    }

    /**
     * @param saxWordOfTs
     * @param id
     * @throws java.lang.Exception
     */
    @Override
    public void insert(final short[] saxWordOfTs, final int id, final short threshold) throws Exception {

        if(node1.canBeInsert(saxWordOfTs)) {

            if(node1.getType()) {
                node1.insert(saxWordOfTs, id, threshold);
            } else {
                if(node1.getNbrTs() == threshold && ((TerminalNode) node1).
                                                                                  canBeSplit()) {

                    short[][] saxWordsLocal      = ((TerminalNode) node1).getSaxWords();
                    int[]     timeSeriesIDsLocal = ((TerminalNode) node1).getTimeSeriesIDs();
                    short[]   saxWordLocal       = node1.getSaxWord();
                    byte[]    saxCardLocal1      = node1.getSaxCard();
                    byte[]    saxCardLocal2      = node1.getSaxCard().clone();
                    node1 = null;

                    int newSplitSymbol = PartitionsUtil.getSymbolForIndexing(saxWordsLocal, saxCardLocal2);

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
                        node1 = new InternalNode(tempNode1, tempNode2, saxWordLocal, saxCardLocal1);

                        for(int i = 0; i < saxWordsLocal.length; i++) {
                            node1.
                                         insert(saxWordsLocal[i], timeSeriesIDsLocal[i], threshold);
                        }

                        node1.insert(saxWordOfTs, id, threshold);

                    } else {
                        node1 = new TerminalNode(saxWordsLocal, timeSeriesIDsLocal, saxWordLocal, saxCardLocal1);
                        node1.insert(saxWordOfTs, id, threshold);
                    }

                } else {
                    node1.insert(saxWordOfTs, id, threshold);
                }

            }

        } else {
            if(node2.canBeInsert(saxWordOfTs)) {

                if(node2.getType()) {
                    node2.insert(saxWordOfTs, id, threshold);
                } else {
                    if(node2.getNbrTs() == threshold && ((TerminalNode) node2).canBeSplit()) {

                        short[][] saxWordsLocal = ((TerminalNode) node2).getSaxWords();
                        int[] timeSeriesIDsLocal = ((TerminalNode) node2).getTimeSeriesIDs();
                        short[] saxWordLocal  = node2.getSaxWord();
                        byte[]  saxCardLocal1 = node2.getSaxCard();
                        byte[]  saxCardLocal2 = node2.getSaxCard().clone();
                        node2 = null;

                        int newSplitSymbol = PartitionsUtil.getSymbolForIndexing(saxWordsLocal, saxCardLocal2);

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

                            node2 = new InternalNode(tempNode1, tempNode2, saxWordLocal, saxCardLocal1);

                            for(int i = 0; i < saxWordsLocal.length; i++) {
                                node2.
                                             insert(saxWordsLocal[i], timeSeriesIDsLocal[i], threshold);
                            }

                            node2.insert(saxWordOfTs, id, threshold);

                        } else {
                            node2 = new TerminalNode(saxWordsLocal, timeSeriesIDsLocal, saxWordLocal, saxCardLocal1);
                            node2.insert(saxWordOfTs, id, threshold);
                        }

                    } else {
                        node2.insert(saxWordOfTs, id, threshold);
                    }

                }

            } else {

                String str1 = "";
                String str2 = "";
                String str3 = "";
                String str4 = "";

                for(int i = 0; i < saxWordOfTs.length; i++) {
                    str1 += (saxWordOfTs[i] >>> node1.getSaxCard()[i]) + ",";
                    str2 += (this.saxWord[i]) + ",";
                    str3 += (node1.getSaxWord()[i]) + ",";
                    str4 += (node2.getSaxWord()[i]) + ",";
                }

                throw new Exception("\nInternalNode >> Should not have recv'd a ts " + str1 + " of size " + saxWordOfTs.length + " at this InternalNode!!!\n\t[" + str2 + "" + Arrays.toString(this.saxCard) + "]\n node1 =[" + str3 + "" + Arrays.toString(node1.getSaxCard()) + "]\n node2 =[" + str4 + "" + Arrays.toString(node2.getSaxCard()) + "]");
            }
        }

    }

    @Override
    public boolean canBeInsert(short[] saxWordOfTs) {

        for(int i = 0; i < saxWordOfTs.length; i++) {
            if(!((saxWordOfTs[i] >>> this.getSaxCard()[i]) == (this.getSaxWord()[i]))) {
                return false;
            }
        }
        return true;

    }

    @Override
    public String toString() {
        return "InternalNode : \n\t " + Arrays.toString(saxWord) + "__" + Arrays.toString(saxCard) + " \n" + node1.toString() + " \n" + node2.toString(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getNbrTs() {
        return this.node1.getNbrTs() + this.node2.getNbrTs();
    }

    @Override
    public void dropTimeSeriesData() {
        this.node1.dropTimeSeriesData();
        this.node2.dropTimeSeriesData();
    }

    @Override
    public int[] ApproximateSearch(short[] saxWordOfTs) throws Exception {
        if(node1.canBeInsert(saxWordOfTs)) {
            if(node1.getType()) {
                return node1.ApproximateSearch(saxWordOfTs);
            } else {
                if(node1.getNbrTs() <= 0) {
                    return node2.getAllTs();
                } else {
                    return node1.ApproximateSearch(saxWordOfTs);
                }
            }
        } else {
            if(node2.canBeInsert(saxWordOfTs)) {
                if(node2.getType()) {
                    return node2.ApproximateSearch(saxWordOfTs);
                } else {
                    if(node2.getNbrTs() <= 0) {
                        return node1.getAllTs();
                    } else {
                        return node2.ApproximateSearch(saxWordOfTs);
                    }
                }
            } else {
                String str1 = "";
                String str2 = "";
                String str3 = "";
                String str4 = "";

                for(int i = 0; i < saxWordOfTs.length; i++) {
                    str1 += (saxWordOfTs[i] >>> node1.getSaxCard()[i]) + ",";
                    str2 += (this.saxWord[i]) + ",";
                    str3 += (node1.getSaxWord()[i]) + ",";
                    str4 += (node2.getSaxWord()[i]) + ",";
                }


                    throw new Exception("\nInternalNode >> Should not have recv'd a ts " + str1 + " of size " + saxWordOfTs.length + " at this InternalNode!!!\n\t[" + str2 + "" + Arrays.toString(this.saxCard) + "]\n node1 =[" + str3 + "" + Arrays.toString(node1.getSaxCard()) + "]\n node2 =[" + str4 + "" + Arrays.toString(node2.getSaxCard()) + "]");

            }
        }


    }

    @Override
    public int getNbrOfNode() {
        return node1.getNbrOfNode() + node2.getNbrOfNode() + 1;
    }

    @Override
    public int getPro() {
        int x = node1.getPro();
        int y = node2.getPro();
        if(x > y) {
            return x + 1;
        } else {
            return y + 1;
        }
    }

    @Override
    public int[] getAllTs() {
        int[] result = Arrays.copyOf(this.node1.getAllTs(), this.node1.getAllTs().length + this.node2.getAllTs()
                .length);
        System.arraycopy(this.node2.getAllTs(), 0, result, this.node1.getAllTs().length, this.node2.getAllTs().length);

        return result;
    }


    public String print(String prefix, boolean isTail){
        String str = prefix + (isTail ? "└── " : "├── ") + this.getHashCode() +"\n";

        str+= node1.print(prefix + (isTail ? "    " : "│   "), false);
        str+= node2.print(prefix + (isTail ?"    " : "│   "), true);


        return str;


    }
}

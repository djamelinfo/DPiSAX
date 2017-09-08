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
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class TerminalNode extends AbstractNode {

    private short[][] saxWords;

    private int[] timeSeriesIDs;

    private int nbrTimeSeries = 0;

    public TerminalNode(short[] saxWord, byte[] saxCard) {
        super(saxWord, saxCard, false);
    }

    public TerminalNode(short[][] saxWords, int[] timeSeriesIDs, short[] saxWord, byte[] saxCard) {
        super(saxWord, saxCard, false);
        this.saxWords = saxWords;
        this.timeSeriesIDs = timeSeriesIDs;
    }

    public short[][] getSaxWords() {
        return saxWords;
    }

    public int[] getTimeSeriesIDs() {
        return timeSeriesIDs;
    }

    @Override
    public void insert(final short[] saxWordOfTs, final int id, final short threshold) throws Exception {

        if(this.saxWords == null) {
            this.saxWords = new short[threshold][saxWordOfTs.length];
            this.timeSeriesIDs = new int[threshold];
        }

        if(nbrTimeSeries >= this.timeSeriesIDs.length) {

            if(true) {
                short[][] saxWordsLocal = new short[saxWords.length * 2][saxWordOfTs.length];

                System.arraycopy(saxWords, 0, saxWordsLocal, 0, saxWords.length);

                saxWords = saxWordsLocal;
            }

            if(true) {
                int[] timeSeriesIDsLocal = new int[timeSeriesIDs.length * 2];

                System.arraycopy(timeSeriesIDs, 0, timeSeriesIDsLocal, 0, timeSeriesIDs.length);

                timeSeriesIDs = timeSeriesIDsLocal;
            }

        }
        try {
            this.timeSeriesIDs[nbrTimeSeries] = id;
            this.saxWords[nbrTimeSeries] = saxWordOfTs;
        } catch(Exception e) {

            throw new Exception("nbrTimeSeries = " + nbrTimeSeries + " this.timeSeriesIDs = " + this.timeSeriesIDs.length + " this.saxWords.length = " + this.saxWords.length);
        }

        nbrTimeSeries++;

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
    public int getNbrTs() {
        return nbrTimeSeries;
    }

    public boolean canBeSplit() {
        for(byte aSaxCard : saxCard) {
            if(aSaxCard != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "TerminalNode :\n\t " + Arrays.toString(saxWord) + "__" + Arrays.
                                                                                       toString(saxCard) + " \n\t nbrOfTs = " + nbrTimeSeries; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int nbrTS() {
        return nbrTimeSeries;
    }

    @Override
    public void dropTimeSeriesData() {
        this.saxWords = null;
    }

    public double EuclideanDistance(short[] ts1, short[] ts2, int from, int to) {
        double dist = 0;

        if(to > ts1.length || to > ts2.length) {
            System.err.println("EuclideanDistance err");
        }
        for(int i = from; i < to; i++) {
            dist += Math.pow(ts1[i] - ts2[i], 2);
        }
        return Math.sqrt(dist);

    }

    @Override
    public int[] ApproximateSearch(short[] saxWordOfTs) {

//        TreeMap<Double, Integer> distMap = new TreeMap<>();
//
//        for(int i = 0; i < this.nbrTimeSeries; i++) {
//            distMap.put(EuclideanDistance(saxWordOfTs, saxWords[i], 0, saxWordOfTs.length), timeSeriesIDs[i]);
//        }
//
//        int   i   = 0;
//        int[] ids = new int[10];
//
//        for(Map.Entry<Double, Integer> entrySet : distMap.entrySet()) {
//            if(i < 10) {
//                ids[i] = entrySet.getValue();
//                i++;
//            } else {
//                break;
//            }
//
//        }

        return timeSeriesIDs;
    }

    @Override
    public int getNbrOfNode() {
        return 1; //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getPro() {
        return 1; //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public int[] getAllTs() {
        return timeSeriesIDs;
    }


    public String print(String prefix, boolean isTail){
        return prefix + (isTail ? "└── " : "├── ") + this.getHashCode() +" "+ this.nbrTimeSeries+"\n" ;



    }
}

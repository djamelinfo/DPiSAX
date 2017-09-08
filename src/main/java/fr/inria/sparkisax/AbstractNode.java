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

import java.io.Serializable;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public abstract class AbstractNode implements Serializable, Cloneable {

    protected short[] saxWord;
    protected byte[]  saxCard;

    // nodeType = false -----> NODE TERMINAL, The leaf node type.
    // nodeType = true -----> NODE INTERNAL, The inner node type.
    private boolean nodeType;

    public AbstractNode(short[] saxWord, byte[] saxCard, boolean nodeType) {
        this.saxWord = saxWord.clone();
        this.saxCard = saxCard.clone();
        this.nodeType = nodeType;
    }

    public byte[] getSaxCard() {
        return saxCard;
    }

    public short[] getSaxWord() {
        return saxWord;
    }

    public boolean getType() {
        return nodeType;
    }

    /**
     * @param saxWordOfTs
     * @param id
     * @param threshold
     * @throws Exception
     */
    public void insert(final short[] saxWordOfTs, final int id, final short threshold) throws Exception {

    }

    public boolean canBeInsert(final short[] saxWordOfTs) {
        return false;
    }

    public int getNbrTs() {
        return 0;
    }

    public String getHashCode() {
        String str = "";
        for(short aSaxWord : saxWord) {
            str += aSaxWord;
        }

        return str;
    }

    public int nbrTS() {
        return 0;
    }

    public void dropTimeSeriesData() {

    }

    public int[] ApproximateSearch(final short[] saxWordOfTs) throws Exception {
        return null;
    }

    public int getNbrOfNode() {
        return 0;
    }

    public int getPro() {
        return 0;
    }


    public int[] getAllTs(){
        return null;
    }

    public String print(String prefix, boolean isTail){

        return null;


    }

}

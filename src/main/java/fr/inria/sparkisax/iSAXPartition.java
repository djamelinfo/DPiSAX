package fr.inria.sparkisax;

import java.io.Serializable;
import java.util.Arrays;

/*
 * Copyright 2016 djamel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * @author djamel
 */
public class iSAXPartition implements Serializable, Cloneable {

    private byte[] saxCard;

    public iSAXPartition(int size, int partitionID) {
        this.saxCard = new byte[size];

        Arrays.fill(saxCard, (byte) 9);
        this.partitionID = partitionID;

    }

    private int partitionID;

    public iSAXPartition(byte[] saxCard) {
        this.saxCard = saxCard;
    }

    public byte[] getSaxCard() {
        return saxCard;
    }

    public void setSaxCard(byte[] saxCard) {
        this.saxCard = saxCard;
    }

    public iSAXPartition(byte[] saxCard, int partitionID) {

        this.saxCard = saxCard;
        this.partitionID = partitionID;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    @Override
    public String toString() {
        return " \t partitionID = " + partitionID + " Card = " + Arrays.
                                                                               toString(saxCard); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected iSAXPartition clone() throws CloneNotSupportedException {
        return new iSAXPartition(saxCard.clone(), partitionID); //To change body of generated methods, choose Tools | Templates.
    }

    public boolean canBeSplit() {
        for(byte aSaxCard : saxCard) {
            if(aSaxCard == 9) {
                return true;
            }
        }
        return false;
    }

}

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

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public final class iSAXSplit {

    public static int getSplitsSymbol(final short[][] saxWords, final byte[] saxCard) {

        short[] nbrOf1 = new short[saxCard.length];

        for(int i = 0; i < saxCard.length; i++) {
            if(saxCard[i] != 0) {
                short tempVal = (short) (saxWords[0][i] >>> (saxCard[i] - 1));
                for(int j = 1; j < saxWords.length; j++) {
                    if((short) (saxWords[j][i] >>> (saxCard[i] - 1)) == tempVal) {
                        nbrOf1[i] += 1;
                    }
                }
            }
        }

        int min = -1;

        // get first index
        for(int i = 0; i < saxCard.length; i++) {
            if(saxCard[i] != 0) {
                min = i;
                break;
            }
        }

        int minClon = min;

        //
        for(int i = min + 1; i < nbrOf1.length && min != -1; i++) {
            if(saxCard[i] != 0) {
                if(((saxWords.length - nbrOf1[i]) - nbrOf1[i]) < ((saxWords.length - nbrOf1[min]) - nbrOf1[min])) {
                    min = i;
                }
            }
        }

        if(min == minClon) {
            for(int i = min + 1; i < (saxCard.length - 1) && min != -1; i++) {

                if(saxCard[min] < saxCard[i]) {
                    min = i;
                }

            }
        }

        return min;

    }








}

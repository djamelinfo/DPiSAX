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
import java.util.List;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class Configuration implements Serializable {

    // sampling config
    private float fraction;
    private short partitions;

    // iSAX config
    private boolean normalization;
    private short   wordLen;
    private int     timeSeriesLength;
    public  short   threshold;

    // Query config
    private short k;

    public Configuration() {
        this.fraction = 1f;
        this.partitions = 256;
        this.normalization = true;
        this.wordLen = 8;
        this.timeSeriesLength = 256;
        this.threshold = 1000;
        this.k = 10;
        this.symbolNotation = new boolean[this.wordLen];
    }

    public Configuration(List<String> configList) {

        System.out.println("\nChange given configuration variables:");
        for(String configList1 : configList) {

            String[] temp = configList1.split("=");
            switch(temp[0]) {
                case "fraction":
                    this.fraction = Float.parseFloat(temp[1]);
                    System.out.println("\t fraction = " + this.fraction);
                    break;
                case "partitions":
                    this.partitions = Short.parseShort(temp[1]);
                    System.out.println("\t partitions = " + this.partitions);
                    break;
                case "k":
                    this.k = Short.parseShort(temp[1]);
                    System.out.println("\t k = " + this.k);
                    break;
                case "normalization":
                    this.normalization = Boolean.parseBoolean(temp[1]);
                    System.out.println("\t normalization = " + this.normalization);
                    break;
                case "wordLen":
                    this.wordLen = Short.parseShort(temp[1]);
                    System.out.println("\t wordLen = " + this.wordLen);
                    break;
                case "timeSeriesLength":
                    this.timeSeriesLength = Integer.parseInt(temp[1]);
                    System.out.println("\t timeSeriesLength = " + this.timeSeriesLength);
                    break;
                case "threshold":
                    this.threshold = Short.parseShort(temp[1]);
                    System.out.println("\t threshold = " + this.threshold);
                    break;
            }

        }

        System.out.println("");

    }

    /**
     * @return the fraction
     */
    public float getFraction() {
        return fraction;
    }

    /**
     * @return the k
     */
    public short getK() {
        return k;
    }

    /**
     * @return the symbolNotation
     */
    public boolean[] getSymbolNotation() {
        return symbolNotation;
    }

    /**
     * @param symbolNotation the symbolNotation to set
     */
    public void setSymbolNotation(boolean[] symbolNotation) {
        this.symbolNotation = symbolNotation;
    }

    /**
     * @return the threshold
     */
    public short getThreshold() {
        return threshold;
    }

    /**
     * @return the timeSeriesLength
     */
    public int getTimeSeriesLength() {
        return timeSeriesLength;
    }

    /**
     * @return the normalization
     */
    public boolean isNormalization() {
        return normalization;
    }

    /**
     * @return the wordLen
     */
    public int getWordLen() {
        return wordLen;
    }

    /**
     * @return the partitions
     */
    public int getPartitions() {
        return partitions;
    }

    public void changeConfig(String str) {

        String[] temp = str.split("=");
        switch(temp[0]) {
            case "fraction":
                this.fraction = Float.parseFloat(temp[1]);
                System.out.println("\t fraction = " + this.fraction);
                break;
            case "partitions":
                this.partitions = Short.parseShort(temp[1]);
                System.out.println("\t partitions = " + this.partitions);
                break;
            case "k":
                this.k = Byte.parseByte(temp[1]);
                System.out.println("\t k = " + this.k);
                break;
            case "normalization":
                this.normalization = Boolean.parseBoolean(temp[1]);
                System.out.println("\t normalization = " + this.normalization);
                break;
            case "wordLen":
                this.wordLen = Short.parseShort(temp[1]);
                System.out.println("\t wordLen = " + this.wordLen);
                break;
            case "timeSeriesLength":
                this.timeSeriesLength = Integer.parseInt(temp[1]);
                System.out.println("\t timeSeriesLength = " + this.timeSeriesLength);
                break;
            case "threshold":
                this.threshold = Short.parseShort(temp[1]);
                System.out.println("\t threshold = " + this.threshold);
                break;
        }

    }

    private boolean[] symbolNotation;

    public void printConfig() {
        System.out.println("Change given configuration variables:");
        System.out.println("\t fraction = " + this.fraction);
        System.out.println("\t partitions = " + this.partitions);
        System.out.println("\t k = " + this.k);
        System.out.println("\t normalization = " + this.normalization);
        System.out.println("\t wordLen = " + this.wordLen);
        System.out.println("\t timeSeriesLength = " + this.timeSeriesLength);
        System.out.println("\t threshold = " + this.threshold);

    }

}

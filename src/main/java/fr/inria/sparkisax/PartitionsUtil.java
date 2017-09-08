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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Djamel Edine YAGOUBI <djamel-edine.yagoubi@inria.fr>
 */
public class PartitionsUtil implements Serializable {

    private static final double[] bp2   = {255.5, 255.5};
    private static final double[] bp4   = {127.5, 127.5, 383.5, 383.5};
    private static final double[] bp8   = {63.5, 63.5, 191.5, 191.5, 319.5, 319.5, 447.5, 447.5};
    private static final double[] bp16  = {31.5, 31.5, 95.5, 95.5, 159.5, 159.5, 223.5, 223.5, 287.5, 287.5, 351.5, 351.5, 415.5, 415.5, 479.5, 479.5};
    private static final double[] bp32  = {15.5, 15.5, 47.5, 47.5, 79.5, 79.5, 111.5, 111.5, 143.5, 143.5, 175.5, 175.5, 207.5, 207.5, 239.5, 239.5, 271.5, 271.5, 303.5, 303.5, 335.5, 335.5, 367.5, 367.5, 399.5, 399.5, 431.5, 431.5, 463.5, 463.5, 495.5, 495.5};
    private static final double[] bp64  = {7.5, 7.5, 23.5, 23.5, 39.5, 39.5, 55.5, 55.5, 71.5, 71.5, 87.5, 87.5, 103.5, 103.5, 119.5, 119.5, 135.5, 135.5, 151.5, 151.5, 167.5, 167.5, 183.5, 183.5, 199.5, 199.5, 215.5, 215.5, 231.5, 231.5, 247.5, 247.5, 263.5, 263.5, 279.5, 279.5, 295.5, 295.5, 311.5, 311.5, 327.5, 327.5, 343.5, 343.5, 359.5, 359.5, 375.5, 375.5, 391.5, 391.5, 407.5, 407.5, 423.5, 423.5, 439.5, 439.5, 455.5, 455.5, 471.5, 471.5, 487.5, 487.5, 503.5, 503.5};
    private static final double[] bp128 = {3.5, 3.5, 11.5, 11.5, 19.5, 19.5, 27.5, 27.5, 35.5, 35.5, 43.5, 43.5, 51.5, 51.5, 59.5, 59.5, 67.5, 67.5, 75.5, 75.5, 83.5, 83.5, 91.5, 91.5, 99.5, 99.5, 107.5, 107.5, 115.5, 115.5, 123.5, 123.5, 131.5, 131.5, 139.5, 139.5, 147.5, 147.5, 155.5, 155.5, 163.5, 163.5, 171.5, 171.5, 179.5, 179.5, 187.5, 187.5, 195.5, 195.5, 203.5, 203.5, 211.5, 211.5, 219.5, 219.5, 227.5, 227.5, 235.5, 235.5, 243.5, 243.5, 251.5, 251.5, 259.5, 259.5, 267.5, 267.5, 275.5, 275.5, 283.5, 283.5, 291.5, 291.5, 299.5, 299.5, 307.5, 307.5, 315.5, 315.5, 323.5, 323.5, 331.5, 331.5, 339.5, 339.5, 347.5, 347.5, 355.5, 355.5, 363.5, 363.5, 371.5, 371.5, 379.5, 379.5, 387.5, 387.5, 395.5, 395.5, 403.5, 403.5, 411.5, 411.5, 419.5, 419.5, 427.5, 427.5, 435.5, 435.5, 443.5, 443.5, 451.5, 451.5, 459.5, 459.5, 467.5, 467.5, 475.5, 475.5, 483.5, 483.5, 491.5, 491.5, 499.5, 499.5, 507.5, 507.5};
    private static final double[] bp256 = {1.5, 1.5, 5.5, 5.5, 9.5, 9.5, 13.5, 13.5, 17.5, 17.5, 21.5, 21.5, 25.5, 25.5, 29.5, 29.5, 33.5, 33.5, 37.5, 37.5, 41.5, 41.5, 45.5, 45.5, 49.5, 49.5, 53.5, 53.5, 57.5, 57.5, 61.5, 61.5, 65.5, 65.5, 69.5, 69.5, 73.5, 73.5, 77.5, 77.5, 81.5, 81.5, 85.5, 85.5, 89.5, 89.5, 93.5, 93.5, 97.5, 97.5, 101.5, 101.5, 105.5, 105.5, 109.5, 109.5, 113.5, 113.5, 117.5, 117.5, 121.5, 121.5, 125.5, 125.5, 129.5, 129.5, 133.5, 133.5, 137.5, 137.5, 141.5, 141.5, 145.5, 145.5, 149.5, 149.5, 153.5, 153.5, 157.5, 157.5, 161.5, 161.5, 165.5, 165.5, 169.5, 169.5, 173.5, 173.5, 177.5, 177.5, 181.5, 181.5, 185.5, 185.5, 189.5, 189.5, 193.5, 193.5, 197.5, 197.5, 201.5, 201.5, 205.5, 205.5, 209.5, 209.5, 213.5, 213.5, 217.5, 217.5, 221.5, 221.5, 225.5, 225.5, 229.5, 229.5, 233.5, 233.5, 237.5, 237.5, 241.5, 241.5, 245.5, 245.5, 249.5, 249.5, 253.5, 253.5, 257.5, 257.5, 261.5, 261.5, 265.5, 265.5, 269.5, 269.5, 273.5, 273.5, 277.5, 277.5, 281.5, 281.5, 285.5, 285.5, 289.5, 289.5, 293.5, 293.5, 297.5, 297.5, 301.5, 301.5, 305.5, 305.5, 309.5, 309.5, 313.5, 313.5, 317.5, 317.5, 321.5, 321.5, 325.5, 325.5, 329.5, 329.5, 333.5, 333.5, 337.5, 337.5, 341.5, 341.5, 345.5, 345.5, 349.5, 349.5, 353.5, 353.5, 357.5, 357.5, 361.5, 361.5, 365.5, 365.5, 369.5, 369.5, 373.5, 373.5, 377.5, 377.5, 381.5, 381.5, 385.5, 385.5, 389.5, 389.5, 393.5, 393.5, 397.5, 397.5, 401.5, 401.5, 405.5, 405.5, 409.5, 409.5, 413.5, 413.5, 417.5, 417.5, 421.5, 421.5, 425.5, 425.5, 429.5, 429.5, 433.5, 433.5, 437.5, 437.5, 441.5, 441.5, 445.5, 445.5, 449.5, 449.5, 453.5, 453.5, 457.5, 457.5, 461.5, 461.5, 465.5, 465.5, 469.5, 469.5, 473.5, 473.5, 477.5, 477.5, 481.5, 481.5, 485.5, 485.5, 489.5, 489.5, 493.5, 493.5, 497.5, 497.5, 501.5, 501.5, 505.5, 505.5, 509.5, 509.5};
    private static final double[] bp512 = {0.5, 0.5, 2.5, 2.5, 4.5, 4.5, 6.5, 6.5, 8.5, 8.5, 10.5, 10.5, 12.5, 12.5, 14.5, 14.5, 16.5, 16.5, 18.5, 18.5, 20.5, 20.5, 22.5, 22.5, 24.5, 24.5, 26.5, 26.5, 28.5, 28.5, 30.5, 30.5, 32.5, 32.5, 34.5, 34.5, 36.5, 36.5, 38.5, 38.5, 40.5, 40.5, 42.5, 42.5, 44.5, 44.5, 46.5, 46.5, 48.5, 48.5, 50.5, 50.5, 52.5, 52.5, 54.5, 54.5, 56.5, 56.5, 58.5, 58.5, 60.5, 60.5, 62.5, 62.5, 64.5, 64.5, 66.5, 66.5, 68.5, 68.5, 70.5, 70.5, 72.5, 72.5, 74.5, 74.5, 76.5, 76.5, 78.5, 78.5, 80.5, 80.5, 82.5, 82.5, 84.5, 84.5, 86.5, 86.5, 88.5, 88.5, 90.5, 90.5, 92.5, 92.5, 94.5, 94.5, 96.5, 96.5, 98.5, 98.5, 100.5, 100.5, 102.5, 102.5, 104.5, 104.5, 106.5, 106.5, 108.5, 108.5, 110.5, 110.5, 112.5, 112.5, 114.5, 114.5, 116.5, 116.5, 118.5, 118.5, 120.5, 120.5, 122.5, 122.5, 124.5, 124.5, 126.5, 126.5, 128.5, 128.5, 130.5, 130.5, 132.5, 132.5, 134.5, 134.5, 136.5, 136.5, 138.5, 138.5, 140.5, 140.5, 142.5, 142.5, 144.5, 144.5, 146.5, 146.5, 148.5, 148.5, 150.5, 150.5, 152.5, 152.5, 154.5, 154.5, 156.5, 156.5, 158.5, 158.5, 160.5, 160.5, 162.5, 162.5, 164.5, 164.5, 166.5, 166.5, 168.5, 168.5, 170.5, 170.5, 172.5, 172.5, 174.5, 174.5, 176.5, 176.5, 178.5, 178.5, 180.5, 180.5, 182.5, 182.5, 184.5, 184.5, 186.5, 186.5, 188.5, 188.5, 190.5, 190.5, 192.5, 192.5, 194.5, 194.5, 196.5, 196.5, 198.5, 198.5, 200.5, 200.5, 202.5, 202.5, 204.5, 204.5, 206.5, 206.5, 208.5, 208.5, 210.5, 210.5, 212.5, 212.5, 214.5, 214.5, 216.5, 216.5, 218.5, 218.5, 220.5, 220.5, 222.5, 222.5, 224.5, 224.5, 226.5, 226.5, 228.5, 228.5, 230.5, 230.5, 232.5, 232.5, 234.5, 234.5, 236.5, 236.5, 238.5, 238.5, 240.5, 240.5, 242.5, 242.5, 244.5, 244.5, 246.5, 246.5, 248.5, 248.5, 250.5, 250.5, 252.5, 252.5, 254.5, 254.5, 256.5, 256.5, 258.5, 258.5, 260.5, 260.5, 262.5, 262.5, 264.5, 264.5, 266.5, 266.5, 268.5, 268.5, 270.5, 270.5, 272.5, 272.5, 274.5, 274.5, 276.5, 276.5, 278.5, 278.5, 280.5, 280.5, 282.5, 282.5, 284.5, 284.5, 286.5, 286.5, 288.5, 288.5, 290.5, 290.5, 292.5, 292.5, 294.5, 294.5, 296.5, 296.5, 298.5, 298.5, 300.5, 300.5, 302.5, 302.5, 304.5, 304.5, 306.5, 306.5, 308.5, 308.5, 310.5, 310.5, 312.5, 312.5, 314.5, 314.5, 316.5, 316.5, 318.5, 318.5, 320.5, 320.5, 322.5, 322.5, 324.5, 324.5, 326.5, 326.5, 328.5, 328.5, 330.5, 330.5, 332.5, 332.5, 334.5, 334.5, 336.5, 336.5, 338.5, 338.5, 340.5, 340.5, 342.5, 342.5, 344.5, 344.5, 346.5, 346.5, 348.5, 348.5, 350.5, 350.5, 352.5, 352.5, 354.5, 354.5, 356.5, 356.5, 358.5, 358.5, 360.5, 360.5, 362.5, 362.5, 364.5, 364.5, 366.5, 366.5, 368.5, 368.5, 370.5, 370.5, 372.5, 372.5, 374.5, 374.5, 376.5, 376.5, 378.5, 378.5, 380.5, 380.5, 382.5, 382.5, 384.5, 384.5, 386.5, 386.5, 388.5, 388.5, 390.5, 390.5, 392.5, 392.5, 394.5, 394.5, 396.5, 396.5, 398.5, 398.5, 400.5, 400.5, 402.5, 402.5, 404.5, 404.5, 406.5, 406.5, 408.5, 408.5, 410.5, 410.5, 412.5, 412.5, 414.5, 414.5, 416.5, 416.5, 418.5, 418.5, 420.5, 420.5, 422.5, 422.5, 424.5, 424.5, 426.5, 426.5, 428.5, 428.5, 430.5, 430.5, 432.5, 432.5, 434.5, 434.5, 436.5, 436.5, 438.5, 438.5, 440.5, 440.5, 442.5, 442.5, 444.5, 444.5, 446.5, 446.5, 448.5, 448.5, 450.5, 450.5, 452.5, 452.5, 454.5, 454.5, 456.5, 456.5, 458.5, 458.5, 460.5, 460.5, 462.5, 462.5, 464.5, 464.5, 466.5, 466.5, 468.5, 468.5, 470.5, 470.5, 472.5, 472.5, 474.5, 474.5, 476.5, 476.5, 478.5, 478.5, 480.5, 480.5, 482.5, 482.5, 484.5, 484.5, 486.5, 486.5, 488.5, 488.5, 490.5, 490.5, 492.5, 492.5, 494.5, 494.5, 496.5, 496.5, 498.5, 498.5, 500.5, 500.5, 502.5, 502.5, 504.5, 504.5, 506.5, 506.5, 508.5, 508.5, 510.5, 510.5};

    public static double[] getBP(byte card) throws Exception {

        switch(card) {
            case 0:
                return bp512;
            case 1:
                return bp256;
            case 2:
                return bp128;
            case 3:
                return bp64;
            case 4:
                return bp32;
            case 5:
                return bp16;
            case 6:
                return bp8;
            case 7:
                return bp4;
            case 8:
                return bp2;
            default:
                throw new Exception("Invalid alphabet size " + card);
        }

    }

    public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator = new Comparator<K>() {
            public int compare(K k1, K k2) {
                int compare = map.get(k2).compareTo(map.get(k1));
                if(compare == 0) {
                    return 1;
                } else {
                    return compare;
                }
            }
        };
        Map<K, V> sortedByValues = new TreeMap<>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }

    public static Map<String, iSAXPartition> getiSAXPartitions(List<short[]> listSAX, Configuration config) throws Exception {

        HashMap<String, List<short[]>> map          = new HashMap<>();
        HashMap<String, iSAXPartition> PartitionMap = new HashMap<>();

        ValueComparator                bvc     = new ValueComparator(map);
        TreeMap<String, List<short[]>> mapSort = new TreeMap<>(bvc);

        System.out.println(listSAX.size());

        int symbolToSplit = getSymbol(listSAX, new iSAXPartition(listSAX.get(0).length, 0));

        //System.out.println("symbolToSplit = " + symbolToSplit);
        String key0 = "";
        String key1 = "";

        for(int i = 0; i < config.getWordLen(); i++) {
            if(i == symbolToSplit) {
                key0 += 0 + "_8 ";
                key1 += 1 + "_8 ";
            } else {
                key0 += "-" + " ";
                key1 += "-" + " ";
            }
        }

        PartitionMap.put(key0, new iSAXPartition(listSAX.get(0).length, 0));
        PartitionMap.put(key1, new iSAXPartition(listSAX.get(0).length, 1));
        PartitionMap.get(key0).getSaxCard()[symbolToSplit] -= 1;
        PartitionMap.get(key1).getSaxCard()[symbolToSplit] -= 1;

        //System.out.println(key0);
        //System.out.println(key1);
        LinkedList<short[]> tempList1 = new LinkedList<>();
        LinkedList<short[]> tempList2 = new LinkedList<>();
        map.put(key1, tempList1);
        map.put(key0, tempList2);

        for(short[] listSAX1 : listSAX) {

            if((listSAX1[symbolToSplit] >>> 8) == 0) {
                map.get(key0).add(listSAX1);
            } else {
                map.get(key1).add(listSAX1);
            }
        }

        mapSort.putAll(map);

        boolean b = true;


        while(map.size() < config.getPartitions()) {

            while(b) {
                //System.out.println(firstKey);

                String firstKey = mapSort.firstKey();

                if(PartitionMap.get(firstKey).canBeSplit()) {

                    List<short[]> firstVal = map.get(firstKey);
                    iSAXPartition p        = PartitionMap.get(firstKey).clone();
                    map.remove(firstKey);
                    PartitionMap.remove(firstKey);
                    mapSort.clear();

                    //System.out.println(firstVal.size());
                    if(firstVal.isEmpty()) {
                        for(int i = 0; i < p.getSaxCard().length; i++) {
                            if(p.getSaxCard()[i] == 9) {
                                symbolToSplit = i;
                                break;
                            }
                        }
                    } else {

                        symbolToSplit = getSymbol(firstVal, p);
                    }

                    // System.out.println("symbolToSplit = " + symbolToSplit);
                    List<short[]> tempList0 = new LinkedList<>();
                    List<short[]> tempList  = new LinkedList<>();

                    for(short[] firstVal1 : firstVal) {
                        if((firstVal1[symbolToSplit] >>> 8) == 0) {
                            tempList0.add(firstVal1);
                        } else {
                            if((firstVal1[symbolToSplit] >>> 8) == 1) {
                                tempList.add(firstVal1);
                            } else {
                                System.out.println("OK haw we get here");
                            }
                        }
                    }

                    iSAXPartition p2 = p.clone();

                    p.getSaxCard()[symbolToSplit] -= 1;
                    p2.getSaxCard()[symbolToSplit] -= 1;

                    String[] key2 = firstKey.split(" ").clone();
                    String[] key3 = firstKey.split(" ").clone();

                    key2[symbolToSplit] = "0" + "_" + p.getSaxCard()[symbolToSplit];
                    key3[symbolToSplit] = "1" + "_" + p2.getSaxCard()[symbolToSplit];

                    p2.setPartitionID(p2.getPartitionID() + 10000);
                    String str1 = "";
                    String str2 = "";

                    for(int i = 0; i < config.getWordLen(); i++) {
                        str1 += key2[i] + " ";
                        str2 += key3[i] + " ";
                    }

                    PartitionMap.put(str1, p);
                    PartitionMap.put(str2, p2);

                    map.put(str1, tempList0);
                    map.put(str2, tempList);

                    b = false;

                } else {

                    //List<short[]> firstVal = map.get(firstKey);
                    if(map.get(firstKey).isEmpty()) {

                        for(Map.Entry<String, List<short[]>> entrySet : mapSort.entrySet()) {
                            firstKey = entrySet.getKey();

                            if(PartitionMap.get(firstKey).canBeSplit()) {

                                List<short[]> firstVal = entrySet.getValue();
                                iSAXPartition p        = PartitionMap.get(firstKey).clone();
                                map.remove(firstKey);
                                PartitionMap.remove(firstKey);
                                mapSort.clear();

                                //System.out.println(firstVal.size());
                                for(int i = 0; i < p.getSaxCard().length; i++) {
                                    if(p.getSaxCard()[i] == 9) {
                                        symbolToSplit = i;
                                        break;
                                    }
                                }

                                //System.out.println("symbolToSplit = " + symbolToSplit);
                                List<short[]> tempList0 = new LinkedList<>();
                                List<short[]> tempList  = new LinkedList<>();

                                for(short[] firstVal1 : firstVal) {
                                    if((firstVal1[symbolToSplit] >>> 8) == 0) {
                                        tempList0.add(firstVal1);
                                    } else {
                                        if((firstVal1[symbolToSplit] >>> 8) == 1) {
                                            tempList.add(firstVal1);
                                        } else {
                                            System.out.println("OK haw we get here");
                                        }
                                    }
                                }

                                iSAXPartition p2 = p.clone();

                                p.getSaxCard()[symbolToSplit] -= 1;
                                p2.getSaxCard()[symbolToSplit] -= 1;

                                String[] key2 = firstKey.split(" ").clone();
                                String[] key3 = firstKey.split(" ").clone();

                                key2[symbolToSplit] = "0" + "_" + p.getSaxCard()[symbolToSplit];
                                key3[symbolToSplit] = "1" + "_" + p2.getSaxCard()[symbolToSplit];

                                p2.setPartitionID(p2.getPartitionID() + 10000);
                                String str1 = "";
                                String str2 = "";

                                for(int i = 0; i < config.getWordLen(); i++) {
                                    str1 += key2[i] + " ";
                                    str2 += key3[i] + " ";
                                }

                                PartitionMap.put(str1, p);
                                PartitionMap.put(str2, p2);

                                map.put(str1, tempList0);
                                map.put(str2, tempList);

                                b = false;
                            }
                        }

                    } else {

                        map.remove(firstKey);
                        mapSort.clear();
                        List<short[]> tempList0 = new LinkedList<>();

                        map.put(firstKey, tempList0);

                        b = false;
                    }

                }

                mapSort.putAll(map);

            }

            b = true;

        }

        int i = 0;

        for(Map.Entry<String, iSAXPartition> entrySet : PartitionMap.entrySet()) {
            entrySet.getValue().setPartitionID(i);
            System.out.println(entrySet);
            i++;

        }

        return PartitionMap;

    }

    public static float Mean(short[][] data, int symbol) {
        float sum = 0;

        for(short[] aData : data) {
            sum += aData[symbol];
        }

        return sum / data.length;
    }

    public static float StdDev(short[][] data, int symbol, float mean) {
        float var = 0.0f;

        for(short[] aData : data) {
            var += (aData[symbol] - mean) * (aData[symbol] - mean);
        }

        var /= (data.length - 1);

        return (float) Math.sqrt(var);
    }

    public static int getSymbol(List<short[]> firstVal, iSAXPartition p) throws Exception {
        short[][] l = new short[firstVal.size()][];

        l = firstVal.toArray(l);

        int symbolToSplit = -1;

        //double mean = SAXutils.breakpointsFinal[255];
        for(int i = 0; i < l[0].length; i++) {

            if(p.getSaxCard()[i] == 9) {

                if(symbolToSplit != -1) {

                    float mean                = Mean(l, i);
                    float meanOfSymbolToSplit = Mean(l, symbolToSplit);
                    float stdDev              = StdDev(l, i, mean);

                    /*
                     * System.out.println("symbolToSplit = " + symbolToSplit +
                     * "\t mean = " + mean + "\t meanOfSymbolToSplit = " +
                     * meanOfSymbolToSplit + "\t stdDev = " + stdDev + "\t (mean
                     * - stdDev) = " + Math.abs(mean - (stdDev)) + "\t (mean +
                     * stdDev) = " + (mean + (stdDev)) + " bp = " + bp + "
                     * bpOfSymbolToSplit " + bpOfSymbolToSplit);
                     */
                    if((mean + (3*stdDev)) > 255.5 && Math.abs(mean - (3*stdDev)) < 255.5) {
                        if(Math.abs(mean - 255.5) > Math.abs(meanOfSymbolToSplit - 255.5)) {
                            symbolToSplit = i;
                        }
                    }

                } else {
                    symbolToSplit = i;
                }
            }

        }

        //System.out.println(symbolToSplit);
        // System.out.println(Arrays.toString(p.getSaxCard()));
        return symbolToSplit;

    }

    public static int getSymbolV2(List<short[]> firstVal, iSAXPartition p) throws Exception {
        short[][] l = new short[firstVal.size()][];

        l = firstVal.toArray(l);

        int symbolToSplit = -1;

        //double mean = SAXutils.breakpointsFinal[255];
        for(int i = 0; i < l[0].length; i++) {

            if(p.getSaxCard()[i] > 0) {

                if(symbolToSplit != -1) {

                    float mean                = Mean(l, i);
                    float meanOfSymbolToSplit = Mean(l, symbolToSplit);
                    float stdDev              = StdDev(l, i, mean);

                    double bp                = getBP((byte) (p.getSaxCard()[i] - 1))[l[0][i] >>> p.getSaxCard()[i] - 1];
                    double bpOfSymbolToSplit = getBP((byte) (p.getSaxCard()[symbolToSplit] - 1))[l[0][symbolToSplit] >>> p.getSaxCard()[symbolToSplit] - 1];

                    /*
                     * System.out.println("symbolToSplit = " + symbolToSplit +
                     * "\t mean = " + mean + "\t meanOfSymbolToSplit = " +
                     * meanOfSymbolToSplit + "\t stdDev = " + stdDev + "\t (mean
                     * - stdDev) = " + Math.abs(mean - (stdDev)) + "\t (mean +
                     * stdDev) = " + (mean + (stdDev)) + " bp = " + bp + "
                     * bpOfSymbolToSplit " + bpOfSymbolToSplit);
                     */
                    if((mean + (stdDev)) > bp && Math.abs(mean - (stdDev)) < bp) {
                        if(Math.abs(mean - bp) > Math.abs(meanOfSymbolToSplit - bpOfSymbolToSplit)) {
                            symbolToSplit = i;
                        }
                    }

                } else {
                    symbolToSplit = i;
                }
            }

        }

        //System.out.println(symbolToSplit);
        // System.out.println(Arrays.toString(p.getSaxCard()));
        return symbolToSplit;

    }

    public static Map<String, iSAXPartition> getPartitions(List<short[]> listSAX, int partitions) {

        TreeMap<String, List<short[]>> map          = new TreeMap<>();
        Map<String, Integer>           mapSort      = new TreeMap<>();
        HashMap<String, iSAXPartition> PartitionMap = new HashMap<>();

        System.out.println(listSAX.size());

        for(short[] listSAX1 : listSAX) {

            if(map.containsKey((listSAX1[0] >>> 8) + "")) {
                map.get((listSAX1[0] >>> 8) + "").add(listSAX1);
            } else {
                LinkedList<short[]> tempList = new LinkedList<>();
                tempList.add(listSAX1);
                map.put((listSAX1[0] >>> 8) + "", tempList);
            }

        }

        for(Map.Entry<String, List<short[]>> entrySet : map.entrySet()) {
            mapSort.put(entrySet.getKey(), entrySet.getValue().size());
        }

        Map<String, Integer> mapSortTemp = null;

        while(map.size() < partitions) {

            mapSortTemp = new TreeMap<>();
            mapSortTemp.putAll(mapSort);
            mapSortTemp = sortByValues(mapSortTemp);

            String key = "";
            for(Map.Entry<String, Integer> entrySet : mapSortTemp.entrySet()) {
                if(entrySet.getKey().length() < listSAX.get(0).length) {
                    key = entrySet.getKey();
                    break;
                }
            }

            List<short[]> firstVal = map.get(key);

            map.remove(key);
            mapSort.remove(key);

            List<short[]> tempList0 = new LinkedList<>();
            List<short[]> tempList1 = new LinkedList<>();

            for(short[] firstVal1 : firstVal) {
                if((firstVal1[key.length()] >>> 8) == 0) {
                    tempList0.add(firstVal1);
                } else {
                    if((firstVal1[key.length()] >>> 8) == 1) {
                        tempList1.add(firstVal1);
                    } else {
                        System.out.println("OK haw we get here");
                    }
                }
            }

            map.put(key + "0", tempList0);
            map.put(key + "1", tempList1);
            mapSort.put(key + "0", tempList0.size());
            mapSort.put(key + "1", tempList1.size());

        }

        int i = 0;

        for(Map.Entry<String, Integer> entrySet : mapSortTemp.entrySet()) {

            String str  = "";
            byte[] card = new byte[listSAX.get(0).length];

            for(int j = 0; j < card.length; j++) {
                if(j < entrySet.getKey().length()) {
                    card[j] = 8;
                    str += entrySet.getKey().charAt(j) + "_8 ";
                } else {
                    card[j] = 9;
                    str += "-" + " ";
                }
            }

            PartitionMap.put(str, new iSAXPartition(card, i));

            i++;

            System.out.println(str + "\t" + PartitionMap.get(str));
        }

        return PartitionMap;

    }

    public static Map<String, iSAXPartition> getiSAXPartitionsWithCard(List<short[]> listSAX, Configuration config) throws Exception {


        System.out.println("in : getiSAXPartitionsWithCard");

        HashMap<String, List<short[]>> map          = new HashMap<>();
        HashMap<String, iSAXPartition> PartitionMap = new HashMap<>();

        ValueComparator                bvc     = new ValueComparator(map);
        TreeMap<String, List<short[]>> mapSort = new TreeMap<>(bvc);

        System.out.println(listSAX.size());

        int symbolToSplit = getSymbolV2(listSAX, new iSAXPartition(listSAX.get(0).length, 0));

        // System.out.println("symbolToSplit = " + symbolToSplit);
        String key0 = "";
        String key1 = "";

        for(int i = 0; i < config.getWordLen(); i++) {
            if(i == symbolToSplit) {
                key0 += 0 + "_8 ";
                key1 += 1 + "_8 ";
            } else {
                key0 += "-" + " ";
                key1 += "-" + " ";
            }
        }

        PartitionMap.put(key0, new iSAXPartition(listSAX.get(0).length, 0));
        PartitionMap.put(key1, new iSAXPartition(listSAX.get(0).length, 1));
        PartitionMap.get(key0).getSaxCard()[symbolToSplit] -= 1;
        PartitionMap.get(key1).getSaxCard()[symbolToSplit] -= 1;

        //System.out.println(key0);
        //System.out.println(key1);
        LinkedList<short[]> tempList1 = new LinkedList<>();
        LinkedList<short[]> tempList2 = new LinkedList<>();
        map.put(key1, tempList1);
        map.put(key0, tempList2);

        for(short[] listSAX1 : listSAX) {

            if((listSAX1[symbolToSplit] >>> 8) == 0) {
                map.get(key0).add(listSAX1);
            } else {
                map.get(key1).add(listSAX1);
            }
        }

        mapSort.putAll(map);

        //        for(Map.Entry<String, List<short[]>> entrySet : mapSort.
        //                entrySet()) {
        //            System.out.println(entrySet.getKey() + "\t -> "
        //                    + entrySet.
        //                    getValue().size());
        //
        //        }
        boolean b = true;

        while(map.size() < config.getPartitions()) {

            while(b) {
                //System.out.println(firstKey);

                String firstKey = mapSort.firstKey();

                // if(PartitionMap.get(firstKey).canBeSplit()) {
                List<short[]> firstVal = map.get(firstKey);
                iSAXPartition p        = PartitionMap.get(firstKey).clone();
                map.remove(firstKey);
                PartitionMap.remove(firstKey);
                mapSort.clear();

                symbolToSplit = getSymbolV2(firstVal, p);

                //System.out.println("symbolToSplit = " + symbolToSplit);
                List<short[]> tempList0 = new LinkedList<>();
                List<short[]> tempList  = new LinkedList<>();

                int s = (firstVal.get(0)[symbolToSplit] >>> p.getSaxCard()[symbolToSplit] - 1);

                // System.out.println("s = " + s);
                int n = 0;
                for(short[] firstVal1 : firstVal) {
                    if((firstVal1[symbolToSplit] >>> p.getSaxCard()[symbolToSplit] - 1) == s) {
                        tempList0.add(firstVal1);
                        n++;
                    } else {

                        tempList.add(firstVal1);

                    }
                }

                //System.out.println("size = " + firstVal.size() + " n " + n);
                iSAXPartition p2 = p.clone();

                p.getSaxCard()[symbolToSplit] -= 1;
                p2.getSaxCard()[symbolToSplit] -= 1;

                //System.out.println(firstKey);
                String[] key2 = firstKey.split(" ").clone();
                String[] key3 = firstKey.split(" ").clone();

                //System.out.println(Arrays.toString(key2));
                //System.out.println(Arrays.toString(key3));
                key2[symbolToSplit] = s + "_" + p.getSaxCard()[symbolToSplit];
                key3[symbolToSplit] = (tempList.get(0)[symbolToSplit] >>> p.getSaxCard()[symbolToSplit]) + "_" + p.getSaxCard()[symbolToSplit];

                // System.out.println(p);
                p2.setPartitionID(p2.getPartitionID() + 10000);
                String str1 = "";
                String str2 = "";

                for(int i = 0; i < config.getWordLen(); i++) {
                    str1 += key2[i] + " ";
                    str2 += key3[i] + " ";
                }

                PartitionMap.put(str1, p);
                PartitionMap.put(str2, p2);

                map.put(str1, tempList0);
                map.put(str2, tempList);

                b = false;

                //  } else {
                //List<short[]> firstVal = map.get(firstKey);
                //     map.remove(firstKey);
                //     mapSort.clear();
                //      List<short[]> tempList0 = new LinkedList<>();
                //     map.put(firstKey, tempList0);
                // }
                mapSort.putAll(map);

                //                for(Map.Entry<String, List<short[]>> entrySet : mapSort.
                //                        entrySet()) {
                //                    System.out.println(entrySet.getKey() + "\t -> "
                //                            + entrySet.
                //                            getValue().size());
                //
                //                }
            }

            b = true;

        }

        int i = 0;

        for(Map.Entry<String, iSAXPartition> entrySet : PartitionMap.entrySet()) {
            entrySet.getValue().setPartitionID(i);
            System.out.println(entrySet);
            i++;

        }

        System.out.println("out : getiSAXPartitionsWithCard");

        return PartitionMap;

    }




    public static int getSymbolForIndexing(short[][] l, byte[] card) throws Exception {




        int symbolToSplit = -1;


        for(int i = 0; i < l[0].length; i++) {

            if(card[i] > 0) {

                if(symbolToSplit != -1) {

                    float mean                = Mean(l, i);
                    float meanOfSymbolToSplit = Mean(l, symbolToSplit);
                    float stdDev              = StdDev(l, i, mean);

                    double bp                = getBP((byte) (card[i] - 1))[l[0][i] >>> card[i] - 1];
                    double bpOfSymbolToSplit = getBP((byte) (card[symbolToSplit] - 1))[l[0][symbolToSplit] >>> card[symbolToSplit] - 1];


                    if((mean + (stdDev)) > bp && Math.abs(mean - (stdDev)) < bp) {
                        if(Math.abs(mean - bp) > Math.abs(meanOfSymbolToSplit - bpOfSymbolToSplit)) {
                            symbolToSplit = i;
                        }
                    }

                } else {
                    symbolToSplit = i;
                }
            }

        }

        //System.out.println(symbolToSplit);
        // System.out.println(Arrays.toString(p.getSaxCard()));
        return symbolToSplit;

    }

}

class ValueComparator implements Comparator<String> {

    Map<String, List<short[]>> base;

    public ValueComparator(Map<String, List<short[]>> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    @Override
    public int compare(String a, String b) {
        if(base.get(a).size() >= base.get(b).size()) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}


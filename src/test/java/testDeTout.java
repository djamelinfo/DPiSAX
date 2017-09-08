import java.util.Map;
import java.util.TreeMap;

/**
 * Created by djamel on 13/12/16.
 */
public class testDeTout {


    public static void main(String[] args) throws Exception {

        System.out.println(1400/256);


        TreeMap<Float, Integer> treeMap = new TreeMap<Float, Integer>();

        treeMap.put(2.3f,1);
        treeMap.put(2.1f,2);
        treeMap.put(2.0f,3);
        treeMap.put(2.0001f,4);
        treeMap.put(2.04f,5);
        treeMap.put(2.3f,6);


        for(Map.Entry<Float,Integer> m: treeMap.entrySet()
            ) {
            System.out.println(m);


        }

    }
}

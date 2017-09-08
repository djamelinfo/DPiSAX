

import fr.inria.sparkisax.SAXutils;
import fr.inria.sparkisax.naivepisax.RootNode;
//import org.apache.commons.math3.distribution.NormalDistribution;
//import org.apache.commons.math3.random.JDKRandomGenerator;




/**
 * Created by djamel on 01/12/16.
 */
public class iSAXtest {

//
//   public static float[] randomWalk(int length) {
//      NormalDistribution n = new NormalDistribution(new JDKRandomGenerator(),
//                                                    0, 1); //mean 0 std 1 variance 1
//      float[] ts = new float[length];
//      float[] e = new float[length - 1];
//
//      for(int i = 0; i < e.length; i++) {
//         e[i] = (float) n.sample();
//      }
//      ts[0] = 0;
//      for(int i = 1; i < length; i++) {
//         ts[i] = ts[i - 1] + e[i - 1];
//      }
//
//      return ts;
//   }

   public static void main(String[] args) throws Exception {



      int nbrOfTimeSeries = 100000;
      int timeSeriesLength = 256;

       RootNode rootNode = new RootNode();

       for(int a = 0; a < nbrOfTimeSeries; a++) {


           //rootNode.insert(SAXutils.ConvertSAX(SAXutils.Z_Normalization(randomWalk(timeSeriesLength)), 8), a, (short)
             //      1000);



       }


       System.out.println(rootNode.print());






   }


}

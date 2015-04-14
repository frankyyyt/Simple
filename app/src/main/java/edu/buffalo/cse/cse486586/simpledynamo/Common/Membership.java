package edu.buffalo.cse.cse486586.simpledynamo.Common;

import java.util.ArrayList;
import java.util.Arrays;

import edu.buffalo.cse.cse486586.simpledynamo.Utils.SimpleDynamoUtils;

/**
 * SimpleDynamo
 * <p/>
 * Created by darrenxyli on 4/9/15.
 * Changed by darrenxyli on 4/9/15 1:30 PM.
 */
public class Membership {

    public ArrayList<Integer> REMOTEAVD = new ArrayList<>(Arrays.asList(5562, 5556, 5554, 5558, 5560));
    String begin = SimpleDynamoUtils.genHash(String.valueOf(REMOTEAVD.get(0)));
    String end = SimpleDynamoUtils.genHash(String.valueOf(REMOTEAVD.get(4)));

    public int findCoordinator(String key) {

        String hashKey = (key != null)? SimpleDynamoUtils.genHash(key) : "";

        for (int i = 0; i < REMOTEAVD.size(); i++) {

            // hashkey < 5562 or 5556 or 5554 or 5558 or 5560: return successor port
            // hashkey > 5560: return 5562

            String cur = SimpleDynamoUtils.genHash(String.valueOf(REMOTEAVD.get(i)));

            if (hashKey.compareToIgnoreCase(end) > 0)
                return Integer.getInteger(end);

            if (hashKey.compareToIgnoreCase(cur) < 0)
                return Integer.getInteger(cur);
        }

        return -1;
    }

    public int[] findPreferenceList(String key) {

        int[] r = new int[4];
        String hashKey = (key != null)? SimpleDynamoUtils.genHash(key) : "";

        for (int i = 0; i < REMOTEAVD.size(); i++) {

            // hashkey < 5562 or 5556 or 5554 or 5558 or 5560: return successor port
            // hashkey > 5560: return 5562

            String cur = SimpleDynamoUtils.genHash(String.valueOf(REMOTEAVD.get(i)));

            if (hashKey.compareToIgnoreCase(end) > 0) {
                r[0] = Integer.getInteger(end);
                r[1] = REMOTEAVD.get(0);
                r[2] = REMOTEAVD.get(1);
                r[3] = REMOTEAVD.get(2);
                break;
            }

            if (hashKey.compareToIgnoreCase(cur) < 0) {
                r[0] = Integer.getInteger(cur);
                r[1] = REMOTEAVD.get((i+1)%5);
                r[2] = REMOTEAVD.get((i+2)%5);
                r[3] = REMOTEAVD.get((i+3)%5);
                break;
            }
        }

        return r;
    }
}

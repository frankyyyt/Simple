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
    public ArrayList<String> REMOTEAVD_HASH = new ArrayList<>(Arrays.asList(
            "177ccecaec32c54b82d5aaafc18a2dadb753e3b1",
            "208f7f72b198dadd244e61801abe1ec3a4857bc9",
            "33d6357cfaaf0f72991b0ecd8c56da066613c089",
            "abf0fd8db03e5ecb199a9b82929e9db79b909643",
            "c25ddd596aa7c81fa12378fa725f706d54325d12"
    ));
    String begin = REMOTEAVD_HASH.get(0);
    String end = REMOTEAVD_HASH.get(4);

    public int findCoordinator(String key) {

        String hashKey = (key != null) ? SimpleDynamoUtils.genHash(key) : "";

        for (int i = 0; i < REMOTEAVD.size(); i++) {

            // hashkey < 5562 or 5556 or 5554 or 5558 or 5560: return successor port
            // hashkey > 5560: return 5562

            String cur = REMOTEAVD_HASH.get(i);

            if (hashKey.compareToIgnoreCase(end) > 0)
                return 0;

            if (hashKey.compareToIgnoreCase(cur) < 0)
                return i;
        }

        return -1;
    }

    public int[] findPreferenceList(String key) {

        int[] r = new int[4];
        String hashKey = (key != null) ? SimpleDynamoUtils.genHash(key) : "";

        for (int i = 0; i < REMOTEAVD.size(); i++) {

            // hashkey < 5562 or 5556 or 5554 or 5558 or 5560: return successor port
            // hashkey > 5560: return 5562

            String cur = REMOTEAVD_HASH.get(i);

            if (hashKey.compareToIgnoreCase(end) > 0) {
                r[0] = REMOTEAVD.get(0);
                r[1] = REMOTEAVD.get(1);
                r[2] = REMOTEAVD.get(2);
                r[3] = REMOTEAVD.get(3);
                break;
            }

            if (hashKey.compareToIgnoreCase(cur) < 0) {
                r[0] = REMOTEAVD.get(i % 5);
                r[1] = REMOTEAVD.get((i + 1) % 5);
                r[2] = REMOTEAVD.get((i + 2) % 5);
                r[3] = REMOTEAVD.get((i + 3) % 5);
                break;
            }
        }

        return r;
    }
}

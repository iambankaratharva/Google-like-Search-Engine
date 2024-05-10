package cis5550.test;

import cis5550.flame.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlameGroupBy {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> elements = Arrays.asList("Pune", "pune", "Mumbai", "Chicago", "Seattle", "seattle", "Chicaro098", "seattle123", "Mumhai");
        FlameRDD elementsRdd = ctx.parallelize(elements);
        FlamePairRDD groupByRdd = elementsRdd.groupBy(s -> s.length() < 3 ? s : s.substring(0, 3));

        List<FlamePair> groupByRddResult = groupByRdd.collect();
        Map<String, String> groupByRddResultHashSet = new HashMap<>();
        for (FlamePair pair : groupByRddResult) {
            String key = pair._1();
            String value = pair._2();
            groupByRddResultHashSet.put(key, value);
            ctx.output(key + ":" + value + "\n");
        }
    }
}

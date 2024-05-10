package cis5550.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;

public class FlameCoGroup {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        LinkedList<String> list1 = new LinkedList<String>();
		LinkedList<String> list2 = new LinkedList<String>();
		for (int i=0; i<Integer.valueOf(args[1]); i+=2)
			list1.add(args[3+i]+","+args[4+i]);
		for (int i=0; i<Integer.valueOf(args[2]); i+=2)
			list2.add(args[3+i+Integer.valueOf(args[1])]+","+args[4+i+Integer.valueOf(args[1])]);
		FlamePairRDD rdd1 = ctx.parallelize(list1).mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",")[1]));
		FlamePairRDD rdd2 = ctx.parallelize(list2).mapToPair(s -> new FlamePair(s.split(",")[0], s.split(",")[1]));
		List<FlamePair> out = rdd1.cogroup(rdd2).collect();
		Collections.sort(out);
		String ret = "";
		for (FlamePair p : out) {
			ret = ret + (ret.equals("") ? "" : ",") + "(" + p._1() + ",\"" + sortInnerLists(p._2()) + "\")";
		}
		ctx.output(ret);
    }
    
    private static String sortInnerLists(String str) {
        String[] outerList = str.split("\\],\\[");
        for (int i = 0; i < outerList.length; i++) {
            String cleanElement = outerList[i].replace("[", "").replace("]", "").trim();
            if (!cleanElement.isEmpty()) {
                String[] innerList = cleanElement.split(",");
                for (int j = 0; j < innerList.length; j++) {
                    innerList[j] = innerList[j].trim();
                }
                Arrays.sort(innerList);
                outerList[i] = "[" + String.join(",", innerList) + "]";
            } else {
                outerList[i] = "[]";
            }
        }
        return String.join(",", outerList);
    }
}

package cis5550.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

public class FlameMapPartitions {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        LinkedList<String> list = new LinkedList<String>();
        for (int i=0; i < args.length; i++)
            list.add(args[i]);
        
        FlameRDD rdd = ctx.parallelize(list);

        rdd = rdd.mapPartitions(iterator -> {
            List<String> results = new ArrayList<>();
            while (iterator.hasNext()) {
                String currentItem = iterator.next();
                results.add(currentItem + "_processed");
            }
            return results.iterator();
        });

        List<String> out = rdd.collect();
        Collections.sort(out);

        String result = "";
        for (String s : out)
            result = result + (result.equals("") ? "" : ",") + s;
        
        ctx.output(result);
    }
}

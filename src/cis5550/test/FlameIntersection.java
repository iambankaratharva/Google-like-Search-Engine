package cis5550.test;

import cis5550.flame.*;
import java.util.*;

public class FlameIntersection {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        List<String> elementsOfRdd1 = Arrays.asList(args[0].split(","));
        List<String> elementsOfRdd2 = Arrays.asList(args[1].split(","));

        FlameRDD rdd2 = ctx.parallelize(elementsOfRdd2);
        FlameRDD rdd1 = ctx.parallelize(elementsOfRdd1);
        FlameRDD intersectionRdd1Rdd2 = rdd1.intersection(rdd2);

        List<String> intersectedList = intersectionRdd1Rdd2.collect();
        Collections.sort(intersectedList);
        String intersectionResult = String.join(",", intersectedList);

        ctx.output(intersectionResult);
    }
}

package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FlameSample {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        double sampleFraction = Double.parseDouble(args[0]);
        
        List<String> data = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < 350; i++) {
            int randomNumber = random.nextInt(9999);
            data.add(String.valueOf(randomNumber));
        }

        FlameRDD dataRDD = ctx.parallelize(data);

        FlameRDD sampleOperationRDD = dataRDD.sample(sampleFraction);

        List<String> result = sampleOperationRDD.collect();
        result.forEach(item -> ctx.output(item + " "));
    }
}

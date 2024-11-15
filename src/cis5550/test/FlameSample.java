package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FlameSample {
    public static void run(FlameContext ctx, String args[]) throws Exception {
        LinkedList<String> list = new LinkedList<String>();
        list.addAll(Arrays.asList(args));

        FlameRDD rdd = ctx.parallelize(list).sample(0.5);

        List<String> out = rdd.collect();

        if (out.size() == 0) {
            ctx.output("No output");
            return;
        }

        String result = "";
        for (String s : out) {
            if (s == null) {
                result = result + (result.equals("") ? "" : ",") + "null";
            } else {
                result = result + (result.equals("") ? "" : ",") + s;
            }
        }

        ctx.output(result);
    }
}

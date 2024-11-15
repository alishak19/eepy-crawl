package cis5550.test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class TestProcessRunner {
    private static final String theJarName = "HW6/lib/kvs.jar:HW6/lib/webserver.jar:out/production/24fa-CIS5550-kunlizhang";

    public static final List<String[]> theArgs = List.of(
            new String[] {theJarName, "cis5550.kvs.Coordinator", "8000"},
            new String[] {theJarName, "cis5550.kvs.Worker", "8001", "worker1", "localhost:8000"},
            new String[] {theJarName, "cis5550.flame.Coordinator", "9000", "localhost:8000"},
            new String[] {theJarName, "cis5550.flame.Worker", "9001", "localhost:9000"},
            new String[] {theJarName, "cis5550.flame.Worker", "9002", "localhost:9000"
            });

    public static void main(String[] args) {
        List<Process> theProcesses = new LinkedList<>();

        for (String[] myArg : theArgs) {
            try {
                List<String> myNewProcess = new LinkedList<>();
                myNewProcess.add("java");
                myNewProcess.add("-cp");
                myNewProcess.addAll(Arrays.asList(myArg));

                ProcessBuilder pb = new ProcessBuilder(myNewProcess);
                pb.inheritIO();
                theProcesses.add(pb.start());
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        for (Process myProcess : theProcesses) {
            try {
                int exitCode = myProcess.waitFor(); // Wait for the process to finish
                System.out.println("Process finished with exit code: " + exitCode);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

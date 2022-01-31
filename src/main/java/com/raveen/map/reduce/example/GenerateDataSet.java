package com.raveen.map.reduce.example;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * This class is used to generate a dataset which contains students ids with marks.
 */
public class GenerateDataSet {

    public static void generateStudentMarks(int dataSetLimit) throws IOException {
        PrintWriter writer = new PrintWriter("student-marks.txt", "UTF-8");
        for (int i=0; i < dataSetLimit; i ++) {
            String studentId = getSaltString(8);
            int mark1 = generateRandomMark(0, 100);
            int mark2 = generateRandomMark(0, 100);
            int mark3 = generateRandomMark(0, 100);
            int mark4 = generateRandomMark(0, 100);
            int mark5 = generateRandomMark(0, 100);

            writer.println(studentId + "," + mark1 + "," + mark2 + "," + mark3 + "," + mark4 + "," + mark5);
        }
        writer.close();
    }

    private static String getSaltString(int length) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) {
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        return salt.toString();
    }

    private static int generateRandomMark(int low, int high) {
        Random r = new Random();
        return r.nextInt(high - low) + low;
    }

}

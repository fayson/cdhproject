package com.cloudera;

import java.io.*;
import java.text.DecimalFormat;
import java.util.Random;

/**
 * package: com.cloudera
 * describe: TODO
 * creat_user: Fayson
 * email: htechinfo@163.com
 * creat_date: 2018/4/27
 * creat_time: 上午11:45
 * 公众号：Hadoop实操
 */
public class GenerateDaily {


    public static void main(String[] args) {
        if(args.length < 3) {
            System.exit(1);
        }
        String sourcepath = args[0];  //用于生成测试数据的基础数据
        String desPath = args[1];   //生成目标测试数据
        int count = Integer.parseInt(args[2]);  //用于生成测试数据的次数
//        String filePath = "/Users/zoulihan/Desktop/ods_user_600.txt";

        readFileByLines(sourcepath, desPath, count);

    }


    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static void readFileByLines(String sourcepath, String desPath, int count) {
        File file = new File(sourcepath);
        BufferedReader reader = null;
        FileWriter fw = null;

        try {
            File desfile = new File(desPath);
            if (!file.exists()) {
                desfile.createNewFile();
            }
            fw = new FileWriter(desPath);

            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            String daliyInfo = "";
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line: " + line);
                for(int i = 0; i < count; i++) {
                    daliyInfo = generateDaily(tempString);
                    fw.write(daliyInfo + "\r\n");
                }
                line++;
            }
            reader.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
            if(fw != null) {
                try {
                    fw.close();
                } catch (IOException e){

                }
            }

        }
    }

    /**
     * 获取货币类型
     * @return
     */
    private static String getMoneyType() {
        int index = RandomUserInfo.getNum(0, Cities.moneyType.length-1);

        return Cities.moneyType[index];
    }

    /**
     * 获取一个随机的Double数值
     * @throws Exception
     */
    private static String generatingDoubleBounded_withRange() throws Exception {
        double min = 1.0;
        double max = 100000.0;
        double boundedDouble = min + new Random().nextDouble() * (max - min);
        DecimalFormat df = new DecimalFormat("#.000");
        return df.format(boundedDouble);
    }


    public static String generateDaily(String line) {
        StringBuffer dailyInfo = new StringBuffer();

        try {
            //用户信息
            String[] userInfo = line.split("\001");
            //生成银行卡号
            String[] creditcardnumbers = RandomCreditCardNumberGenerator.generateMasterCardNumbers(1);
            String creditcardnumber = creditcardnumbers[0];
            //货币类型
            String moneyType = getMoneyType();
            String money = generatingDoubleBounded_withRange();
            String balance = generatingDoubleBounded_withRange();
            String punching = generatingDoubleBounded_withRange();
            String memo = RandomUserInfo.getRoad() + "test---xxx";

            dailyInfo.append(userInfo[0]).append(",")
                    .append(creditcardnumber).append(",")
                    .append(moneyType).append(",")
                    .append(money).append(",")
                    .append(balance).append(",")
                    .append(punching).append(",")
                    .append(memo);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return dailyInfo.toString();
    }
}

package com.potato.moviePro;

/**
 * Created by potato on 2018/5/7.
 */
public class TextSimilarity {

    /**
     * 求三个数中最小的一个
     * @param one
     * @param two
     * @param three
     * @return
     */
    private static int min(int one, int two, int three) {
        int min = one;
        if (two < min) {
            min = two;
        }
        if (three < min) {
            min = three;
        }
        return min;
    }

    /**
     * 求编辑距离(Edit Distance)
     * @param str1
     * @param str2
     * @return 编辑距离
     */
    private static int editDistance(String str1, String str2) {
        int d[][]; // 矩阵
        int y = str1.length();
        int x = str2.length();
        char ch1; // str1的
        char ch2; // str2的
        int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        if (y == 0) {
            return x;
        }
        if (x == 0) {
            return y;
        }
        d = new int[y + 1][x + 1]; // 计算编辑距离二维数组
        for (int j = 0; j <= x; j++) { // 初始化编辑距离二维数组第一行
            d[0][j] = j;
        }
        for (int i = 0; i <= y; i++) { // 初始化编辑距离二维数组第一列
            d[i][0] = i;
        }
        for (int i = 1; i <= y; i++) { // 遍历str1
            ch1 = str1.charAt(i - 1);
            // 去匹配str2
            for (int j = 1; j <= x; j++) {
                ch2 = str2.charAt(j - 1);
                if (ch1 == ch2) {
                    temp = 0;
                } else {
                    temp = 1;
                }
                // 左边+1,上边+1, 左上角+temp取最小
                d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp);
            }
        }
        return d[y][x];
    }

    /**
     * 计算相似度
     * @param str1
     * @param str2
     * @return
     */
    public static double similar(String str1, String str2) {
        int ed = editDistance(str1, str2);
        return 1 - (double) ed / Math.max(str1.length(), str2.length());
    }

    public static void main(String[] args) {
        String str1 = "北京市东城区景山前街4号故宫故";
        String str2 = "上海市浦东新区故宫";
        System.out.println("字符串相似度: " + new TextSimilarity().similar(str1, str2));
    }
}

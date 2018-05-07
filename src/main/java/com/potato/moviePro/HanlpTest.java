package com.potato.moviePro;

import com.hankcs.hanlp.HanLP;

/**
 * Created by potato on 2018/5/5.
 */
public class HanlpTest {

    public static void main(String[] args) {
        String text = "";
        //String traditionText= "比妳聰明的人，請不要讓他還比妳努力";
        System.out.println(HanLP.segment(text));  //分词

        System.out.println(HanLP.extractKeyword(text,2));  //提取关键字，同时指定提取的个数
        System.out.println(HanLP.extractKeyword(text,1));  //提取关键字，同时指定提取的个数
        //System.out.println(HanLP.extractPhrase(text,2));  //提取短语,，同时指定提取的个数
        //System.out.println(HanLP.extractSummary(text,2));  //提取摘要，同时指定提取的个数

        //System.out.println(HanLP.getSummary(text,10));  //提取短语，同时指定摘要的最大长度

        //System.out.println(HanLP.convertToTraditionalChinese(text));  //简体字转为繁体字
        //System.out.println(HanLP.convertToSimplifiedChinese(traditionText));  //繁体字转为简体字
        //System.out.println(HanLP.convertToPinyinString(text," ",false));  //转为拼音
    }

}

package com.potato.moviePro;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**预选相似度算法
 * Created by potato on 2018/5/4.
 */
public class CosineSimilarAlgorithm {
    /**
     *
     * @Title: cosSimilarityByFile
     * @Description: 获取两个文件相似性
     * @param @param firstFile
     * @param @param secondFile
     * @param @return
     * @return Double
     * @throws
     */
//    public static Double cosSimilarityByFile(String firstFile,String secondFile){
//        try{
//            Map<String, Map<String, Integer>> firstTfMap=TfIdfAlgorithm.wordSegCount(firstFile);
//            Map<String, Map<String, Integer>> secondTfMap=TfIdfAlgorithm.wordSegCount(secondFile);
//            if(firstTfMap==null || firstTfMap.size()==0){
//                throw new IllegalArgumentException("firstFile not found or firstFile is empty! ");
//            }
//            if(secondTfMap==null || secondTfMap.size()==0){
//                throw new IllegalArgumentException("secondFile not found or secondFile is empty! ");
//            }
//            Map<String,Integer> firstWords=firstTfMap.get(firstFile);
//            Map<String,Integer> secondWords=secondTfMap.get(secondFile);
//            if(firstWords.size()<secondWords.size()){
//                Map<String, Integer> temp=firstWords;
//                firstWords=secondWords;
//                secondWords=temp;
//            }
//            return calculateCos((LinkedHashMap<String, Integer>)firstWords, (LinkedHashMap<String, Integer>)secondWords);
//
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        return 0d;
//    }

    /**
     *
     * @Title: cosSimilarityByString
     * @Description: 得到两个字符串的相似性
     * @param @param first
     * @param @param second
     * @param @return
     * @return Double
     * @throws
     */
//    public static Double cosSimilarityByString(String first,String second){
//        try{
//            Map<String, Integer> firstTfMap=TfIdfAlgorithm.segStr(first);
//            Map<String, Integer> secondTfMap=TfIdfAlgorithm.segStr(second);
//            if(firstTfMap.size()<secondTfMap.size()){
//                Map<String, Integer> temp=firstTfMap;
//                firstTfMap=secondTfMap;
//                secondTfMap=temp;
//            }
//            return calculateCos((LinkedHashMap<String, Integer>)firstTfMap, (LinkedHashMap<String, Integer>)secondTfMap);
//
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        return 0d;
//    }

    /**
     *
     * @Title: calculateCos
     * @Description: 计算余弦相似性
     * @param @param first
     * @param @param second
     * @param @return
     * @return Double
     * @throws
     */
    public static Double calculateCos(LinkedHashMap<String, Integer> first,LinkedHashMap<String, Integer> second){

        List<Map.Entry<String, Integer>> firstList = new ArrayList<Map.Entry<String, Integer>>(first.entrySet());
        List<Map.Entry<String, Integer>> secondList = new ArrayList<Map.Entry<String, Integer>>(second.entrySet());
        //计算相似度
        double vectorFirstModulo = 0.00;//向量1的模
        double vectorSecondModulo = 0.00;//向量2的模
        double vectorProduct = 0.00; //向量积
        int secondSize=second.size();
        for(int i=0;i<firstList.size();i++){
            if(i<secondSize){
                vectorSecondModulo+=secondList.get(i).getValue().doubleValue()*secondList.get(i).getValue().doubleValue();
                vectorProduct+=firstList.get(i).getValue().doubleValue()*secondList.get(i).getValue().doubleValue();
            }
            vectorFirstModulo+=firstList.get(i).getValue().doubleValue()*firstList.get(i).getValue().doubleValue();
        }
        return vectorProduct/(Math.sqrt(vectorFirstModulo)*Math.sqrt(vectorSecondModulo));
    }

    public static void main(String[] args){
//        Double result=cosSimilarityByString("中国是超级大国",
//                "中国是世界超级大国。");
//        System.out.println(result);
    }
}

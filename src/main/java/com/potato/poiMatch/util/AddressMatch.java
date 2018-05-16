package com.potato.poiMatch.util;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;

import java.util.List;

/**地址相似度计算
 * Created by potato on 2018/5/10.
 */
public class AddressMatch {
    public static void main(String[] args) {
        List<Term> termList = HanLP.segment("中国科学院计算技术研究所的宗成庆教授正在教授自然语言处理计算课程");
        System.out.println(termList);
    }

    public Double AddressSim(String add1,String add2){
        //先进行分词
        return  null;
    }
}

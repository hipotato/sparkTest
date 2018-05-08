package com.potato.moviePro;

/**
 * Created by potato on 2018/5/7.
 */
public class GeoNumUtil {

    public static int GeoNumSameByte(Long a,Long b){

        int i =0;
        while(i<64){
            if(a>>>63-i==b>>>63-i){
                i++;
            }else break;
        }
        return  i;
    }

    public static void main(String[] args) {
        Long a = 409649456299687936L;
        Long b = 409625054137335808L;
        System.out.println(GeoNumSameByte(a,b));
    }
}

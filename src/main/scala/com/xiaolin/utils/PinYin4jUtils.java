package xiaolin.utils;
import java.util.Arrays;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

public class PinYin4jUtils {
    /**
     * 将字符转换成拼音 获取首字母 大写
     *
     * @param hanyu
     * @return String
     */
    public static String getFirstPinYin(String hanyu) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(HanyuPinyinCaseType.UPPERCASE);
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);

        StringBuilder firstPinyin = new StringBuilder();
        char[] hanyuArr = hanyu.trim().toCharArray();
        try {
            for (int i = 0, len = hanyuArr.length; i < len; i++) {
                if(Character.toString(hanyuArr[i]).matches("[\\u4E00-\\u9FA5]+")){
                    String[] pys = PinyinHelper.toHanyuPinyinStringArray(hanyuArr[i],format);
                    firstPinyin.append(pys[0].charAt(0));
                }else {
                    firstPinyin.append(hanyuArr[i]);
                }
            }
        } catch (BadHanyuPinyinOutputFormatCombination badHanyuPinyinOutputFormatCombination) {
            badHanyuPinyinOutputFormatCombination.printStackTrace();
        }
        return firstPinyin.toString();
    }


    public static void main(String[] args) {
        // pin4j 简码 和 城市编码
        String s1 = "瓯海 我的程（A6-2a、A6-3a地块）";
        String headArray = getFirstPinYin(s1); // 获得每个汉字拼音首字母
        System.out.println(headArray);
//
//        String s2 ="长城" ;
//        System.out.println(Arrays.toString(stringToPinyin(s2,true,",")));
//
//        String s3 ="长";
//        System.out.println(Arrays.toString(stringToPinyin(s3,true,",")));
    }
}
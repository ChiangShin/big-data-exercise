package com.chiangshin.hadoop.hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Author jx
 * @Date 2019/9/3 23:38
 */
public class DateFormaterUDF extends UDF {

    private final SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yy:HH:mm:ss", Locale.ENGLISH);
    private final SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");

    public Text evaluate(Text str){
        Text outputText = new Text();

        if(str == null){
            return null;
        }

        if (StringUtils.isBlank(str.toString())){
            return null;
        }

        try {
            Date parseDate = inputFormat.parse(str.toString().trim());
            String outDate = outputFormat.format(parseDate);
            outputText.set(outDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return outputText;
    }
}

package com.snort.intelli.app.config;

import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;

public class BlankLineRecordSeparatorPolicy extends SimpleRecordSeparatorPolicy {


    @Override
    public boolean isEndOfRecord(String line) {
        return line.trim().length()!=0 && super.isEndOfRecord(line);
    }

    @Override
    public String postProcess(String record) {

        if(record==null || record.trim().length()==0){
            return null;
        }
        return super.postProcess(record);
    }
}

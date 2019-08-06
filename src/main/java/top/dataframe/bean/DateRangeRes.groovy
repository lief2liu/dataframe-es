package top.dataframe.bean

import groovy.transform.CompileStatic

@CompileStatic
class DateRangeRes {
    private String start
    private String end
    private String dsl

    String getStart() {
        return start
    }

    String getEnd() {
        return end
    }

    String getDsl() {
        return dsl
    }

    DateRangeRes(String start, String end, String dsl) {
        this.start = start
        this.end = end
        this.dsl = dsl
    }
}
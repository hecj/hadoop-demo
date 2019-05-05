package com.hadoop.mr.page.topn;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PageCount implements Comparable<PageCount>{

    private String page;
    private int count;

    @Override
    public int compareTo(PageCount o) {
        return o.getCount()-this.count==0?
                this.page.compareTo(o.getPage()):o.getCount()-this.count;
    }
}

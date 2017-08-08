package com.tatsuya;

import java.io.Serializable;
import java.util.Comparator;

public class X implements Serializable, Comparator<String>{

    @Override
    public int compare(String s, String t1) {
        return Integer.valueOf(s.length()).compareTo(Integer.valueOf(t1.length()));
    }
}

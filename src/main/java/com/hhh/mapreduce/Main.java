package com.hhh.mapreduce;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by nicbh on 2017/7/13.
 */
public class Main {
    public static void main(String[] args) throws Exception{
        try {
            String name = args[0];
            ArrayList<String> argslist = new ArrayList<String>(Arrays.asList(args));
            argslist.remove(0);
            args = argslist.toArray(new String[argslist.size()]);
            Class class1 = Class.forName("com.hhh.mapreduce." + name);
            Method method = class1.getMethod("main", String[].class);
            method.invoke(null, (Object) args);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException ex) {
            ex.printStackTrace();
            System.err.println("Usage: mainClassName arguments");
            System.exit(2);
        }
    }
}

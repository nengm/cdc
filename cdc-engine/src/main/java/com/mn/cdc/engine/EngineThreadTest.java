package com.mn.cdc.engine;

import com.mn.cdc.config.Configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-10-20 11:20
 **/
public class EngineThreadTest {
    public static void main(String[] args) throws IOException, SQLException {
        Properties properties = new Properties();
        FileInputStream in = null;
        try{
            in = new FileInputStream(Thread.currentThread().getContextClassLoader().getResource("config.properties").getPath());
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }
        try {
            properties.load(in);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            in.close();
        }
        Configuration config = Configuration.from(properties);
        EngineThread engineThread = new EngineThread(config);
        Thread thread = new Thread(engineThread);
        thread.start();
    }
}

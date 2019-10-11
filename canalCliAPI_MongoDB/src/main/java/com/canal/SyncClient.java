/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal;

import com.canal.helper.MongoDBHelper;

/**
 *
 * @author sheriff
 */
public class SyncClient {
    public static void main(String[] args){
        MongoDBHelper m=new MongoDBHelper();
        System.out.println(String.valueOf(m.dropTable("test","abc")));
    }
}

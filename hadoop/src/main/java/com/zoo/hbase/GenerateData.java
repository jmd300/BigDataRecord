package com.zoo.hbase;

import lombok.AllArgsConstructor;

import java.util.Random;
/**
 * @Author: JMD
 * @Date: 8/1/2023
 */
public class GenerateData {
    // 创建一个Random对象
    Random random = new Random();

    @AllArgsConstructor
    static class User{
        String userId;
        Integer age;
    }

    @AllArgsConstructor
    static class Behavior{
        String userId;
        String action;

        long stamp;
    }

    @AllArgsConstructor
    static class Item{
        String itemId;

        Double price;
    }
    String generateId(){
        return String.valueOf(random.nextInt());
    }

    String generateUserId(){
        return String.valueOf(random.nextInt(100));
    }
    String generateItemId(){
        return String.valueOf(random.nextInt(1000));
    }

    Behavior generateBehavior(){
        String[] behaviors = {"click", "buy", "view", "collect", "like"};
        int index = random.nextInt(5);
        long timeStamp = System.currentTimeMillis();
        return new Behavior(generateUserId(), behaviors[index], timeStamp);
    }

    User generateUser(){
        return new User(generateId(), random.nextInt(50) + 10);
    }

    Item generateItem(){
        return new Item(generateId(), random.nextDouble() * 30);
    }

    void generateBehaviorData(){

    }

    void generateUserData(){

    }
    void generateItemData(){

    }
    public static void main(String[] args) {

    }
}

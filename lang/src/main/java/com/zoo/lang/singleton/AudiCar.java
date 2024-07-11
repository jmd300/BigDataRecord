package com.zoo.lang.singleton;

public enum AudiCar {
    INSTANCE;

    String name = "Audi";
    void run(){
        System.out.println(name + " run ... ");
    }
}

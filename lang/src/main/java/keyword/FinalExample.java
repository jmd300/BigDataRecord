package keyword;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

public class FinalExample {
    @Data
    @AllArgsConstructor
    @ToString
    static class People{
        String name;
        int age;
    }

    public static void main(String[] args) {
        final People people = new People("ming", 20);
        people.setAge(22);
        System.out.println(people);
    }
}

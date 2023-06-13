package com.zoo.lang.collecting;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static java.util.stream.Collectors.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * @Author: JMD
 * @Date: 6/13/2023
 */
public class DownStreamCollectors {
    @AllArgsConstructor
    @Getter
    public static class City{
        private String name;
        private String state;

        private int population;
    }

    public static Stream<City> readCities(String filename)throws IOException{
        return Files.lines(Paths.get(filename))
                .map(l -> l.split(","))
                .map(a -> new City(a[0], a[1], Integer.parseInt(a[2])));
    }
    public static void main(String[] args) throws IOException {
        Stream<Locale> locales = Stream.of(Locale.getAvailableLocales());
        // locales.forEach(System.out::println);
        Map<String, Set<Locale>> countryToLocaleSet = locales.collect(groupingBy(Locale::getCountry, toSet()));
        System.out.println("countryToLocaleSet: " + countryToLocaleSet);

        locales = Stream.of(Locale.getAvailableLocales());
        Map<String, Long> CountryToLocaleCounts = locales.collect(groupingBy(Locale::getCountry, counting()));
        System.out.println("CountryToLocaleCounts: " + CountryToLocaleCounts);

        locales = Stream.of(Locale.getAvailableLocales());
        System.out.println(locales.map(Locale::getCountry).filter(e -> e == null || e.length() == 0).count());

        Stream<City> cities =  readCities("input/cities.txt");
        Map<String, Optional<String>> stateToLongestCItyName = cities
                .collect(groupingBy(City::getState, mapping(City::getName, maxBy(Comparator.comparing(String::length)))));
        System.out.println("stateToLongestCItyName: " + stateToLongestCItyName);

        locales = Stream.of(Locale.getAvailableLocales());
        Map<String, Set<String>> countryToLanguages = locales.collect(
                groupingBy(
                        Locale::getDisplayCountry,
                        mapping(Locale::getDisplayLanguage, toSet())
                ));
        System.out.println("countryToLanguages: " + countryToLanguages);

        cities =  readCities("input/cities.txt");
        Map<String, IntSummaryStatistics> stateToCityPopulationSummary = cities.collect(groupingBy(City::getState, summarizingInt(City::getPopulation)));
        System.out.println("河北 stateToCityPopulationSummary: " + stateToCityPopulationSummary.get("河北"));
        System.out.println("北京 stateToCityPopulationSummary: " + stateToCityPopulationSummary.get("北京"));

        cities =  readCities("input/cities.txt");
        Map<String, String> stateToCityNames = cities.collect(groupingBy(
                City::getState,
                reducing("", City::getName, (s, t) -> s.length() == 0 ? t : s + ", " + t)
        ));

        System.out.println("stateToCityNames: "  + stateToCityNames);

        cities =  readCities("input/cities.txt");
        stateToCityNames = cities.collect(groupingBy(
                City::getState,
                mapping(City::getName, joining(", "))
        ));
        System.out.println("stateToCityNames: "  + stateToCityNames);
    }
}

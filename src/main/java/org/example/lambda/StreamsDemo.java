package org.example.lambda;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamsDemo {
    public static void somemethod() {
        //java 7
        List<Integer> arr = new ArrayList<Integer>();
        for(int i = 1; i < 501; i++) {
            arr.add(i);
        }
        List<Integer> squares7 = new ArrayList<Integer>();
        for(int x : arr ) {
            squares7.add( x * x);
          }
        //java 8
        //IntStream range = IntStream.range(1, 501);
        Stream<Integer> squares = (Stream<Integer>) arr.stream().map(x -> {
            System.out.println(x);
            return x* x;
        });
        List<Integer> squaresList = squares.collect(Collectors.toList());
        Stream<Integer> squares2 = arr.stream().map((x) -> x * x).filter((x) -> x > 100);
        List<Integer> squares8 =  squares2.collect(Collectors.toList());
        Stream<Integer> out = arr.parallelStream().filter((x) -> x % 2 == 0 ).map((x) -> x * x);
        List<Integer> squaresParallel = out.collect(Collectors.toList());
    }

    public static void main(String[] args) {
        //somemethod();
        List<String> items = new ArrayList<String>();
        items.add("one");
        items.add("two");
        items.add("three");
        Stream<String> stream = items.stream();
        Stream<String> stringStream =
                stream.map((value) -> { return value.toLowerCase(); });
        System.out.println(stringStream.collect(Collectors.joining(",")));
//        long count = items.stream()
//                .map((value) -> { return value.toLowerCase(); })
//                .filter((x) -> x.startsWith("t"))
//                .count();
//
//        System.out.println("count = " + count);
//
//        //flatMap
        List<String> stringList = new ArrayList<String>();
        stringList.add("One flew over the cuckoo's nest");
        stringList.add("To kill a muckingbird");
        stringList.add("Gone with the wind");
        Stream<String> streamFlatMap = stringList.stream();
        streamFlatMap.  //map((value) -> {
                flatMap((value) -> {
            String[] split = value.split(" ");
            return (Stream<String>) Arrays.asList(split).stream();
            //return split;
        })
                .forEach((value) -> System.out.println(value))
        ;
//        List<List<Integer>> marks = new ArrayList<>();
//
//        List<Integer> student1 = new ArrayList<>();
//        student1.add(100);
//        student1.add(50);
//        marks.add(student1);
//
//        List<Integer> student2 = new ArrayList<>();
//        student2.add(70);
//        student2.add(80);
//        marks.add(student2);
//
//        //Stream<Integer> flattenedStreeam = marks.stream().flatMap((x) -> x.stream());
//
//        //Optional<Integer> sumAll = marks.stream().flatMap((x) -> x.stream()).reduce((x, sum) -> x + sum);
//
//        List<Integer> allMarks = marks.stream().flatMap((x) -> {
//            Stream<Integer> y = x.stream();
//            y.map((i) -> i + 5);
//            return y;
//        }).collect(Collectors.toList());
//        System.out.println(allMarks);
    }

}



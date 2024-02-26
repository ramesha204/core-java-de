package org.example.lambda;

import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Predicate;

public class FunctionalCompositionDemo {
    public static void composeDemo() {
        Function<Integer, Integer> multiply = (value) -> value * 2;
        Function<Integer, Integer> add      = (value) -> value + 3;

        Function<Integer, Integer> compposeMultiply = multiply.compose(add);
        Function<Integer, Integer> compose2 = compposeMultiply
                .compose(add)
                .compose(multiply)
                .compose((value) -> value * value);

        Integer result1 = compposeMultiply.apply(3);
        System.out.println(result1);
        Integer result2 =compose2.apply(2);
        System.out.println(result2);
    }

    public static void andThenDemo() {

        Function<Long, String> convertToStringFunction = (value) -> Long.toString(value);
        LongFunction<String> convertToStringLong = (value) -> Long.toString(value);
        long l = 500l;

        convertToStringFunction.apply(l);
        convertToStringLong.apply(l);


        ////
        Function<Integer, Integer> multiply = (value) -> value * 2;
        Function<Integer, Integer> add      = (value) -> value + 3;

        Function<Integer, Integer> multiplyThenAdd = multiply.andThen(add)
                .andThen((value) -> value * value);

        Integer result2 = multiplyThenAdd.apply(3);
        System.out.println(result2);
    }

    public static void predicateCompositionDemo() {
        Predicate<String> startsWithA = (text) -> text.startsWith("A");
        Predicate<String> endsWithX   = (text) -> text.endsWith("x");

        Predicate<String> composedAnd = startsWithA.and(endsWithX);

        String input = "A hardworking person must relax";

        Predicate<String> composedOr = startsWithA.or(endsWithX);

        System.out.println("Composed AND : " + composedAnd.test(input));
        System.out.println("Composed OR : " + composedOr.test(input));

    }

    public static void main(String[] args) {
        composeDemo();
       andThenDemo();
//        predicateCompositionDemo();
    }
}

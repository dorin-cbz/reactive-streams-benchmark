package com.endava.benchmark.streams.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import io.vertx.core.file.OpenOptions;
import io.vertx.rx.java.RxHelper;
import org.openjdk.jmh.annotations.*;
import rx.Observable;

import java.util.*;
import java.util.concurrent.CountDownLatch;

@State(Scope.Thread)
public class RxVertxTest {

    VertxOptions options = new VertxOptions().setBlockedThreadCheckInterval(10000000L);
    private Vertx vertx;
    AsyncFile from;
    AsyncFile toSimple;
    AsyncFile toMap;
    AsyncFile toMapReduce;
    AsyncFile toComplex;
    AsyncFile toComplexWithoutGroupBy;
    Integer fIndex= 0;

    @Setup
    public void prepareStart() {
        vertx = Vertx.vertx(options);
        from = vertx.fileSystem().openBlocking("data.txt", new OpenOptions());
    }


    @Setup(Level.Iteration)
    public void prepareBenchmark() {
        FileSystem fileSystem = vertx.fileSystem();
        from.setReadPos(0);

        fIndex++;
        String dataSimple = "dataSimple"+(fIndex)+".txt";
        String dataMap = "dataMap"+(fIndex)+".txt";
        String dataMapReduce = "dataMapReduce"+(fIndex)+".txt";
        String dataComplex = "dataComplex"+(fIndex)+".txt";
        String dataComplexWithoutGroupBy = "dataComplexWithoutGroupBy"+(fIndex)+".txt";
        toSimple = fileSystem.openBlocking(dataSimple, new OpenOptions().setCreate(true));
        toMap = fileSystem.openBlocking(dataMap, new OpenOptions().setCreate(true));
        toMapReduce = fileSystem.openBlocking(dataMapReduce, new OpenOptions().setCreate(true));
        toComplex = fileSystem.openBlocking(dataComplex, new OpenOptions().setCreate(true));
        toComplexWithoutGroupBy = fileSystem.openBlocking(dataComplexWithoutGroupBy, new OpenOptions().setCreate(true));
    }

    @TearDown(Level.Iteration)
    public void tearDownBenchmark() {
        toSimple.close();
        toMap.close();
        toMapReduce.close();
        toComplex.close();
        toComplexWithoutGroupBy.close();
    }

    @TearDown
    public void tearDownExit() {
        from.close();
        vertx.close();
    }

//    @Benchmark
//    public void test_simple() throws Exception {
//        CountDownLatch latch = new CountDownLatch(1);
//
//        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
//        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
//                subscribe(e -> {toSimple.write(Buffer.buffer(new byte[]{e}));},
//                        error -> {throw new RuntimeException(error);},
//                        () -> {
//                            latch.countDown();
//                        }
//                );
//
//        latch.await();
//    }

    @Benchmark
    public void test_simpleWithReduce() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i)) ).
                reduce( Buffer.buffer(), (buf, b) -> buf.appendByte(b)).
                filter(b -> b.length()>0).
                subscribe( toSimple::write,
                        error -> {throw new RuntimeException(error);},
                        latch::countDown
                );

        latch.await();
    }

//    @Benchmark
//    public void test_map() throws Exception {
//        CountDownLatch latch = new CountDownLatch(1);
//
//        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
//        observable.concatMap( buf -> Observable.range(0, buf.length()).map( buf::getByte) ).
//                map(b -> Buffer.buffer(new byte[]{b, b})).
//                subscribe( toMap::write,
//                            error -> {throw new RuntimeException(error);},
//                            latch::countDown
//                    );
//
//        latch.await();
//    }

    @Benchmark
    public void test_mapWithReduce() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map( buf::getByte) ).
                //cache(10000).
                reduce(Buffer.buffer(), (buf, b)-> buf.appendBytes(new byte[]{b, b})).
                //map(b -> Buffer.buffer(new byte[]{b, b})).
                filter(b -> b.length()>0).
                subscribe( toMapReduce::write,
                        error -> {throw new RuntimeException(error);},
                        latch::countDown
                );

        latch.await();
    }

    boolean isALetter(int b){
        String c = String.valueOf(b);
        return Character.isAlphabetic(b) && !"\n ,.!:;'-\"".contains(c);
    }


    @Benchmark
    public void test_complex() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(buf::getByte) ).
                publish( b1 -> b1.filter( e -> isALetter(e) ).
                        buffer(() -> b1.filter( e -> !isALetter(e) ))
                ).filter(arr -> !arr.isEmpty()).
                map( arr -> arr.stream().reduce(new StringBuilder(), (acc, b)-> acc.appendCodePoint(b), (acc1, acc2)-> acc1.append(acc2))).
                buffer(2, 1).filter(l -> l.size() > 1).
                groupBy(s -> s .get(0).toString()).flatMap(r -> r.reduce(new HashMap<String, ArrayList>(),
                            (a1, a2) -> {
                                String key = a2.get(0).toString();
                                ArrayList value = a1.get(key);
                                if( value == null){
                                    value = new ArrayList<>();
                                }
                                value.add(a2.get(1));
                                a1.put(key, value);
                                return a1;} ) ).

                reduce(Buffer.buffer(), (buf, b) -> buf.appendBytes(b.toString().getBytes()).appendString("\n")).
                filter(b -> b.length()>0).
                subscribe(toComplex::write,
                        error -> {throw new RuntimeException(error);},
                        latch::countDown
                );

        latch.await();
    }

    @Benchmark
    public void test_complexWithoutGroupBy() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(buf::getByte) ).
                publish( b1 -> b1.filter( e -> isALetter(e) ).
                        buffer(() -> b1.filter( e -> !isALetter(e) ))
                ).filter(arr -> !arr.isEmpty()).
                map( arr -> arr.stream().reduce(new StringBuilder(), (acc, b)-> acc.appendCodePoint(b), (acc1, acc2)-> acc1.append(acc2))).
                buffer(2, 1).filter(l -> l.size() > 1).
//                groupBy(s -> s .get(0).toString()).flatMap(r -> r.reduce(new HashMap<String, ArrayList>(),
//                (a1, a2) -> {
//                    String key = a2.get(0).toString();
//                    ArrayList value = a1.get(key);
//                    if( value == null){
//                        value = new ArrayList<>();
//                    }
//                    value.add(a2.get(1));
//                    a1.put(key, value);
//                    return a1;} ) ).

                reduce(Buffer.buffer(), (buf, b) -> buf.appendBytes(b.toString().getBytes()).appendString("\n")).
                filter(b -> b.length()>0).
                subscribe(toComplexWithoutGroupBy::write,
                        error -> {throw new RuntimeException(error);},
                        latch::countDown
                );

        latch.await();
    }


    public static void main(String args[]) throws Exception {

        RxVertxTest test = new RxVertxTest();
//
//        test.prepareStart();
//        test.prepareBenchmark();
//
//        test.test_simple();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();
//
//        test.prepareStart();
//        test.prepareBenchmark();
//
//        test.test_simpleWithReduce();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();
//
//        test.prepareStart();
//        test.prepareBenchmark();
//
//        test.test_map();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();
//
//        test.prepareStart();
//        test.prepareBenchmark();
//
//        test.test_complex();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();


        test.prepareStart();
        test.prepareBenchmark();

        test.test_complex();

        test.tearDownBenchmark();
        test.tearDownExit();
    }

}
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

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

@State(Scope.Benchmark)
public class RxVertxTest {

    VertxOptions options = new VertxOptions().setBlockedThreadCheckInterval(10000000L);
    private Vertx vertx;
    AsyncFile from;
    AsyncFile toSimple;
    AsyncFile toWithMap;
    AsyncFile toComplex;
    Integer fIndex= 0;

    @Setup
    public void prepareStart() {
        vertx = Vertx.vertx(options);
        FileSystem fileSystem = vertx.fileSystem();
        from = fileSystem.openBlocking("data.txt", new OpenOptions());
    }


    @Setup(Level.Iteration)
    public void prepareBenchmark() {
        FileSystem fileSystem = vertx.fileSystem();

        fIndex++;
        String dataSimple = "dataSimple"+(fIndex)+".txt";
        String dataWithMap = "dataWithMap"+(fIndex)+".txt";
        String dataComplex = "dataComplex"+(fIndex)+".txt";
        new File(dataSimple).delete();
        new File(dataWithMap).delete();
        new File(dataComplex).delete();
        toSimple = fileSystem.openBlocking(dataSimple, new OpenOptions().setCreate(true));
        toWithMap = fileSystem.openBlocking(dataWithMap, new OpenOptions().setCreate(true));
        toComplex = fileSystem.openBlocking(dataComplex, new OpenOptions().setCreate(true));
    }

    @TearDown(Level.Iteration)
    public void tearDownBenchmark() {
        from.setReadPos(0);
        toSimple.close();
        toWithMap.close();
        toComplex.close();
    }

    @TearDown
    public void tearDownExit() {
        from.close();
        vertx.close();
    }

    @Benchmark
    public void test_simple() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
                subscribe(e -> {toSimple.write(Buffer.buffer(new byte[]{e}));},
                        error -> {throw new RuntimeException(error);},
                        () -> {
                            latch.countDown();
                        }
                );

        latch.await();
    }

    @Benchmark
    public void test_simpleWithCache() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
                cache(10000).collect(() -> Buffer.buffer(), (buf, b) -> buf.appendByte(b)).
                filter(b -> b.length()>0).
                subscribe(e -> {toSimple.write(e);},
                        error -> {throw new RuntimeException(error);},
                        () -> {
                            latch.countDown();
                        }
                );

        latch.await();
    }

    @Benchmark
    public void test_withMap() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
                map(b -> Buffer.buffer(new byte[]{b, b})).
                subscribe(e -> {toWithMap.write(e);},
                            error -> {throw new RuntimeException(error);},
                            () -> {
                                latch.countDown();
                            }
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
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
                publish( b1 -> b1.filter(b2 -> isALetter(b2)).
                        buffer(() -> b1.filter(b2 -> !isALetter(b2)))
                ).filter(arr -> !arr.isEmpty()).
                map( arr -> arr.stream().reduce(new StringBuilder(), (acc, b)-> acc.appendCodePoint(b), (acc1, acc2)-> acc1.append(acc2))).
                buffer(2, 1).filter(l -> l.size() > 1).
                subscribe(s -> {
                        String line = s.toString() + "\n";
                        toComplex.write(Buffer.buffer(line.getBytes()));},
                        error -> {throw new RuntimeException(error);},
                        () -> {
                            latch.countDown();
                        }
                );

        latch.await();
    }

    @Benchmark
    public void test_complexWithCache() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
                publish( b1 -> b1.filter(b2 -> isALetter(b2)).
                        buffer(() -> b1.filter(b2 -> !isALetter(b2)))
                ).filter(arr -> !arr.isEmpty()).
                map( arr -> arr.stream().reduce(new StringBuilder(), (acc, b)-> acc.appendCodePoint(b), (acc1, acc2)-> acc1.append(acc2))).
                buffer(2, 1).filter(l -> l.size() > 1).
                cache(10000).collect(() -> Buffer.buffer(), (buf, b) -> buf.appendBytes(b.toString().getBytes()).appendString("\n")).
                filter(b -> b.length()>0).
                subscribe(s -> {
                            toComplex.write(s);},
                        error -> {throw new RuntimeException(error);},
                        () -> {
                            latch.countDown();
                        }
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
//        test.test_simpleWithCache();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();

//        test.prepareStart();
//        test.prepareBenchmark();
//
//        test.test_withMap();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();

//        test.prepareStart();
//        test.prepareBenchmark();
//
//        test.test_complex();
//
//        test.tearDownBenchmark();
//        test.tearDownExit();
        test.prepareStart();
        test.prepareBenchmark();

        test.test_complexWithCache();

        test.tearDownBenchmark();
        test.tearDownExit();
    }

}
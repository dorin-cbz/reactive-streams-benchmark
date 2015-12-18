package com.endava;

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

/**
 * Created by mcanter on 18/12/2015.
 */
@State(Scope.Benchmark)
public class Test {

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
        System.out.println("Start");
    }


    @Setup(Level.Iteration)
    public void prepareBenchmark() {
        System.out.println("Iteration " + ++fIndex);
        FileSystem fileSystem = vertx.fileSystem();

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
        System.out.println("TearDown Benchmark " + fIndex);
        from.setReadPos(0);
        toSimple.close();
        toWithMap.close();
    }

    @TearDown
    public void tearDownExit() {
        System.out.println("TearDownExit");
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
                            //System.out.println("Complete ");
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
                                //System.out.println("Complete ");
                                latch.countDown();
                            }
                    );

        latch.await();
    }

    //@Benchmark
    public void test_complex() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Buffer> observable = RxHelper.toObservable(from.setReadBufferSize(1048576));
        observable.concatMap( buf -> Observable.range(0, buf.length()).map(i -> buf.getByte(i))).
                publish( b1 -> b1.filter(b2 -> !Character.isSpaceChar(b2)).
                        buffer(() -> b1.filter(b2 -> Character.isSpaceChar(b2)))
                ).map( arr -> arr.stream().reduce(new StringBuilder(), (acc, b)-> acc.appendCodePoint(b), (acc1, acc2)-> acc1.append(acc2))).
                buffer(2, 1).
                subscribe(e -> {toComplex.write(Buffer.buffer(e.toString().getBytes()));},
                        error -> {throw new RuntimeException(error);},
                        () -> {
                            //System.out.println("Complete ");
                            latch.countDown();
                        }
                );

        latch.await();
    }

    public static void main(String args[]) throws Exception {

        Test test = new Test();

        test.prepareStart();
        test.prepareBenchmark();
        test.test_simple();

        test.tearDownBenchmark();
        test.tearDownExit();

        test.prepareStart();
        test.prepareBenchmark();

        test.test_withMap();

        test.tearDownBenchmark();
        test.tearDownExit();

        test.prepareStart();
        test.prepareBenchmark();

        //test.test_complex();

        test.tearDownBenchmark();
        test.tearDownExit();
    }

}
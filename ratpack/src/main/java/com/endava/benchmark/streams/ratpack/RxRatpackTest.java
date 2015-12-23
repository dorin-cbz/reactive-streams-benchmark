package com.endava.benchmark.streams.ratpack;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import ratpack.rx.RxRatpack;
import ratpack.test.exec.ExecHarness;
import ratpack.test.handling.RequestFixture;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static ratpack.test.handling.RequestFixture.requestFixture;

@State(Scope.Thread)
public class RxRatpackTest {
    static{
        RxRatpack.initialize();
    }
    Observable<Integer> observable1;
    Observable<Integer> observable1k;
    Observable<Integer> observable1M;
    RequestFixture fixture;

    private static Iterable<Integer> createIntRange(int upper) {
        return ContiguousSet.create(Range.closedOpen(0, upper), DiscreteDomain.integers());
    }

    @Setup
    public void prepare() {

        observable1 = Observable.from(createIntRange(1));
        observable1k = Observable.from(createIntRange(1000));
        observable1M = Observable.from(createIntRange(1000000));


    }
    Observer<Object> createObserver(CountDownLatch latch){
        return new Observer<Object>(){
            @Override
            public void onCompleted() {latch.countDown();}
            @Override
            public void onError(Throwable throwable) {}
            @Override
            public void onNext(Object o) {}
        };
    }
    @Benchmark
    public void test1() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e -> observable1.subscribe(createObserver(latch)));
        latch.await();

    }

    @Benchmark
    public void test1k() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1k.subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void test1M() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1M.subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void testFlatMap1() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1.flatMap(i -> Observable.from(Arrays.asList(i))).subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void testFlatMap1k() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1k.flatMap(i -> Observable.from(Arrays.asList(i))).subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void testFlatMap1M() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1M.flatMap(i -> Observable.from(Arrays.asList(i))).subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void testFlatMap2() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1.flatMap(i -> Observable.from(Arrays.asList(i, i+1))).subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void testFlatMap2k() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        ExecHarness.runSingle(e ->
                observable1k.flatMap(i -> Observable.from(Arrays.asList(i, i+1))).subscribe(createObserver(latch))
        );
        latch.await();

    }

    @Benchmark
    public void testFlatMap2M() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);

        ExecHarness.runSingle(e ->
                observable1M.flatMap(i -> Observable.from(Arrays.asList(i, i+1))).subscribe(createObserver(latch))
        );
        latch.await();

    }

    public static void main(String args[]) throws Exception {
        RxRatpackTest test = new RxRatpackTest();
        test.prepare();
        test.test1();
    }
}
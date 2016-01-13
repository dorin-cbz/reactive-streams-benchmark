package com.endava.benchmark.streams.rxJava;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 2)
@State(Scope.Thread)
public class rxJava {

    @Param({"1", "1000", "1000000"})
    int times;

    Observable<Integer> rxJustSimple;
    Observable<Integer> rxRangeSimple;
    Observable<Integer> rxJust;
    Observable<Integer> rxRange;
    Observable<Integer> rxJustAsync;
    Observable<Integer> rxRangeAsync;

    @Setup(Level.Iteration)
    public void setup() {

        rxJustSimple = Observable.range(0, times);
        rxRangeSimple = Observable.range(0, times);

        rxJust = rxJustSimple.flatMap(Observable::just);
        rxRange = rxRangeSimple.flatMap(v -> Observable.range(v, 2));

        rxJustAsync = rxJust.observeOn(Schedulers.newThread());
        rxRangeAsync = rxRange.observeOn(Schedulers.newThread());

    }

    @Benchmark
    public void rxJustSimple(Blackhole bh) {
        rxJust.subscribe(newSubscriber(bh));
    }

    @Benchmark
    public void rxRangeSimple(Blackhole bh) {
        rxRange.subscribe(newSubscriber(bh));
    }

    @Benchmark
    public void rxJust(Blackhole bh) {
        rxJust.subscribe(newSubscriber(bh));
    }

    @Benchmark
    public void rxRange(Blackhole bh) {
        rxRange.subscribe(newSubscriber(bh));
    }

    @Benchmark
    public void rxJustAsync(Blackhole bh) throws Exception {
        LatchedObserver o = new LatchedObserver(bh);
        rxJustAsync.subscribe(o);
        o.cdl.await();
    }

    @Benchmark
    public void rxRangeAsync(Blackhole bh) throws Exception {
        LatchedObserver o = new LatchedObserver(bh);
        rxRangeAsync.subscribe(o);
        o.cdl.await();
    }

    static public Subscriber newSubscriber(Blackhole bh) {
        return Subscribers.from(
                new Observer<Integer>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                    }

                    @Override
                    public void onNext(Integer t) {
                        bh.consume(t);
                    }
                }

        );
    }

    static final class LatchedObserver<T> implements Observer<T> {

        public CountDownLatch cdl = new CountDownLatch(1);
        private final Blackhole bh;

        public LatchedObserver(Blackhole bh) {
            this.bh = bh;
        }

        @Override
        public void onCompleted() {
            cdl.countDown();
        }

        @Override
        public void onError(Throwable e) {
            cdl.countDown();
        }

        @Override
        public void onNext(T t) {
            bh.consume(t);
        }
    }
}

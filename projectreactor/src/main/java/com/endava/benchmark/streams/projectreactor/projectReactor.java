package com.endava.benchmark.streams.projectreactor;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.rx.Streams;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(value = 2)
@State(Scope.Thread)
public class projectReactor {

    @Param({"1", "1000", "1000000"})
    int times;

    static {
        Environment.initialize();
    }

    Publisher<Long> simple;
    Publisher<Long> rcJust;
    Publisher<Long> rcRange;
    Publisher<Long> rcJustAsync;
    Publisher<Long> rcRangeAsync;

    @Setup(Level.Iteration)
    public void setup() {

        simple = Streams.range(0, times);

        rcJust = Streams.wrap(simple).flatMap(Streams::just);
        rcRange = Streams.wrap(simple).flatMap(v -> Streams.range(v, v).subscribeOn(Environment.workDispatcher()));

        rcJustAsync = Streams.wrap(rcJust).dispatchOn(Environment.cachedDispatcher());
        rcRangeAsync = Streams.wrap(rcRange).dispatchOn(Environment.cachedDispatcher());
    }


    @Benchmark
    public void simple(Blackhole bh) {
        simple.subscribe(createObserver(bh));
    }

    @Benchmark
    public void rcJust(Blackhole bh) {
        rcJust.subscribe(createObserver(bh));
    }

    @Benchmark
    public void rcRange(Blackhole bh) {
        rcRange.subscribe(createObserver(bh));
    }

    //Error
    @Benchmark
    public void simpleErr(Blackhole bh) {
        simple.subscribe(createObserverErr(bh));
    }

    @Benchmark
    public void rcJustErr(Blackhole bh) {
        rcJust.subscribe(createObserverErr(bh));
    }

    @Benchmark
    public void rcRangeErr(Blackhole bh) {
        rcRange.subscribe(createObserverErr(bh));
    }

    //Without Err
    @Benchmark
    public void rcJustAsync(Blackhole bh) throws Exception {
        LatchedObserver o = new LatchedObserver(bh);
        rcJustAsync.subscribe(o);
        o.cdl.await();
    }

    @Benchmark
    public void rcRangeAsync(Blackhole bh) throws Exception {
        LatchedObserver o = new LatchedObserver(bh);
        rcRangeAsync.subscribe(o);
        o.cdl.await();
    }

    //With Err
    @Benchmark
    public void rcJustAsyncErr(Blackhole bh) throws Exception {
        LatchedObserverErr o = new LatchedObserverErr(bh);
        rcJustAsync.subscribe(o);
        o.cdl.await();
    }

    @Benchmark
    public void rcRangeAsyncErr(Blackhole bh) throws Exception {
        LatchedObserverErr o = new LatchedObserverErr(bh);
        rcRangeAsync.subscribe(o);
        o.cdl.await();
    }

    private Subscriber<Object> createObserverErr(Blackhole bh) {
        return new Subscriber<Object>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Object t) {
                bh.consume(t);
                if (((Integer) t) % 2 == 0) throw new ArithmeticException();
            }

            @Override
            public void onError(Throwable t) {
//            t.printStackTrace();
            }

            @Override
            public void onComplete() {
            }
        };
    }

    static final class LatchedObserverErr implements Subscriber<Object> {

        final CountDownLatch cdl;

        final Blackhole bh;

        public LatchedObserverErr(Blackhole bh) {
            cdl = new CountDownLatch(1);
            this.bh = bh;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Object t) {
            bh.consume(t);
            if (((Integer) t) % 2 == 0) throw new ArithmeticException();
        }

        @Override
        public void onError(Throwable t) {
//            t.printStackTrace();
            cdl.countDown();
        }

        @Override
        public void onComplete() {
            cdl.countDown();
        }

    }

    private Subscriber<Object> createObserver(Blackhole bh) {
        return new Subscriber<Object>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Object t) {
                bh.consume(t);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
            }
        };
    }

    static final class LatchedObserver implements Subscriber<Object> {

        final CountDownLatch cdl;

        final Blackhole bh;

        public LatchedObserver(Blackhole bh) {
            cdl = new CountDownLatch(1);
            this.bh = bh;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Object t) {
            bh.consume(t);
        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
            cdl.countDown();
        }

        @Override
        public void onComplete() {
            cdl.countDown();
        }

    }
}

package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.rpc.DataBuffer;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class RemoteSubpartitionTest {

    @Test
    void closeWakesAFlushWaitingForCreditFromADeadWorker() throws Exception {
        RecordingObserver observer = new RecordingObserver();
        RemoteSubpartition partition = new RemoteSubpartition(
                "source#0", "sessions", 0, observer, 16);
        partition.add(new StreamRecord<>("record", 1L));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> flush = executor.submit(() -> {
                observer.flushStarted.countDown();
                partition.flush();
                return null;
            });
            observer.flushStarted.await();
            Thread.sleep(50);
            assertThat(flush.isDone())
                    .as("flush should be waiting for downstream credit before cancellation")
                    .isFalse();

            assertThatCode(partition::close).doesNotThrowAnyException();
            assertThatCode(() -> flush.get(Duration.ofSeconds(1).toMillis(),
                    java.util.concurrent.TimeUnit.MILLISECONDS)).doesNotThrowAnyException();
            assertThat(observer.completed).isTrue();
            assertThat(observer.buffers).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    private static final class RecordingObserver implements StreamObserver<DataBuffer> {
        private final CountDownLatch flushStarted = new CountDownLatch(1);
        private volatile int buffers;
        private volatile boolean completed;

        @Override
        public void onNext(DataBuffer value) {
            buffers++;
        }

        @Override
        public void onError(Throwable failure) {
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}

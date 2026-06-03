package ai.pipestream.registration.handlers;

import io.smallrye.mutiny.Uni;

import java.util.concurrent.ExecutionException;

/**
 * Bridges an external Mutiny {@link Uni} to a blocking value on the current thread.
 *
 * <p>The {@code pipestream-service-registration} Consul helpers expose a Mutiny-only API.
 * This is the single, contained boundary where that {@code Uni} is converted to a plain
 * {@code CompletableFuture} and waited on. Call only from a virtual thread — the block
 * parks the carrier-free virtual thread, never the Vert.x event loop.
 */
final class UniBlocking {

    private UniBlocking() {
    }

    static <T> T await(Uni<T> uni) {
        try {
            return uni.subscribeAsCompletionStage().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause != null ? cause : e);
        }
    }
}

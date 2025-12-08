/*
 * Copyright The WildFly Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.jboss.as.server.suspend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jboss.as.server.logging.ServerLogger;
import org.wildfly.common.Assert;
import org.wildfly.common.function.Functions;

/**
 * Orchestrates suspending and resuming of registered server activity.
 * Registered server activity is organized into groups sharing the same suspend priority.
 * Server suspension happens in two phases: prepare + suspend.
 * <ol>
 * <li>Set state to {@code State#PRE_SUSPEND}</li>
 * <li>Iterate over activity groups in priority order (from first to last). For each group:
 *  <ol>
 *  <li>Create prepare stages for each registered server activity</li>
 *  <li>Once all prepare stages within priority group have complete, move on to next priority group</li>
 *  </ol>
 * </li>
 * <li>Set state to {@code State#SUSPENDING}</li>
 * <li>Iterate over activity groups in priority order (from first to last). For each group:
 *  <ol>
 *  <li>Create suspend stages for each registered server activity</li>
 *  <li>Once all suspend stages within priority group have complete, move on to next priority group</li>
 *  </ol>
 * </li>
 * <li>Set state to {@code State#SUSPENDED}</li>
 * </ol>
 * Resuming the suspended server happens in one phase:
 * <ol>
 * <li>Iterate over activity groups in reverse priority order (from last to first). For each group:
 *  <ol>
 *  <li>Create resume stages for each registered server activity</li>
 *  <li>Once all resume stages within priority group have complete, move on to next priority group</li>
 *  </ol>
 * </li>
 * <li>Set state to {@code State#RUNNING}</li>
 * </ol>
 * @author Stuart Douglas
 * @author Paul Ferraro
 */
public class SuspendController implements ServerSuspendController, SuspendableActivityRegistry {
    private static final Supplier<List<SuspendableActivity>> FACTORY = CopyOnWriteArrayList::new;

    // Server activity groups in suspend priority order (max -> min)
    // Initialized as an unmodifiable list of empty activity lists
    private final List<List<SuspendableActivity>> activityGroups = Stream.generate(FACTORY).limit(SuspendPriority.LAST.ordinal() + 1).collect(Collectors.toUnmodifiableList());
    // Index of activity priorities
    private final Map<SuspendableActivity, SuspendPriority> priorities = Collections.synchronizedMap(new IdentityHashMap<>());

    private final List<OperationListener> listeners = new CopyOnWriteArrayList<>();

    private final AtomicReference<State> state = new AtomicReference<>(State.SUSPENDED);
    private volatile CompletionStage<Void> activeSuspend = SuspendableActivity.COMPLETED;

    @Override
    public void reset() {
        this.state.getAndSet(State.SUSPENDED);
    }

    @Override
    public CompletionStage<Void> suspend(ServerSuspendContext context) {
        System.err.println("[" + Thread.currentThread().getName() + "] suspend() ENTER - current state: " + this.state.get());
        if (!this.state.compareAndSet(State.RUNNING, State.PRE_SUSPEND)) {
            System.err.println("[" + Thread.currentThread().getName() + "] suspend() - CAS failed, returning activeSuspend");
            return this.activeSuspend;
        }
        System.err.println("[" + Thread.currentThread().getName() + "] suspend() - CAS success, now PRE_SUSPEND");
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<Void> result = future.whenComplete((ignore, exception) -> {
            System.err.println("[" + Thread.currentThread().getName() + "] suspend.whenComplete() ENTER - state: " + this.state.get() + ", exception: " + exception);
            if (exception == null) {
                this.state.set(State.SUSPENDED);
                System.err.println("[" + Thread.currentThread().getName() + "] suspend.whenComplete() - state set to SUSPENDED");
                for (OperationListener listener: this.listeners) {
                    try {
                        listener.complete();
                    } catch (Throwable e) {
                        ServerLogger.ROOT_LOGGER.warn(e.getLocalizedMessage(), e);
                    }
                }
            }
            System.err.println("[" + Thread.currentThread().getName() + "] suspend.whenComplete() EXIT - final state: " + this.state.get());
        });
        this.activeSuspend = result;
        for (OperationListener listener: this.listeners) {
            listener.suspendStarted();
        }
        // Collect stages in case we need to cancel them
        List<CompletionStage<Void>> phaseStages = new ArrayList<>(2);
        result.whenComplete(propagateCancellation(phaseStages));
        System.err.println("[" + Thread.currentThread().getName() + "] suspend() - starting phaseStage for PREPARE");
        // Prepare activity groups in priority order, i.e. first -> last
        phaseStages.add(phaseStage(this.activityGroups, SuspendableActivity::prepare, context, (ignored, prepareException) -> {
            System.err.println("[" + Thread.currentThread().getName() + "] suspend PREPARE complete - exception: " + prepareException);
            if (prepareException != null) {
                // If prepare fails, log failure and complete with cancellation
                ServerLogger.ROOT_LOGGER.suspendFailed(prepareException);
                result.completeExceptionally(new CancellationException(prepareException.getMessage()));
            } else {
                this.state.set(State.SUSPENDING);
                System.err.println("[" + Thread.currentThread().getName() + "] suspend() - state set to SUSPENDING, starting SUSPEND phase");
                // Suspend activity groups in priority order, i.e. first -> last order
                phaseStages.add(phaseStage(this.activityGroups, SuspendableActivity::suspend, context, (ignore, suspendException) -> {
                    System.err.println("[" + Thread.currentThread().getName() + "] suspend SUSPEND complete - exception: " + suspendException);
                    if (suspendException != null) {
                        future.completeExceptionally(suspendException);
                    } else {
                        System.err.println("[" + Thread.currentThread().getName() + "] suspend SUSPEND - completing future");
                        future.complete(null);
                    }
                }));
            }
        }));
        System.err.println("[" + Thread.currentThread().getName() + "] suspend() EXIT - returning result future");
        return result;
    }

    @Override
    public CompletionStage<Void> resume(ServerResumeContext context) {
        System.err.println("[" + Thread.currentThread().getName() + "] resume() ENTER - current state: " + this.state.get());
        if (this.state.get() == State.RUNNING) {
            System.err.println("[" + Thread.currentThread().getName() + "] resume() - already RUNNING, returning COMPLETED");
            return SuspendableActivity.COMPLETED;
        }
        // Cancel any active suspend
        this.activeSuspend.toCompletableFuture().cancel(false);
        for (OperationListener listener: this.listeners) {
            listener.cancelled();
        }
        System.err.println("[" + Thread.currentThread().getName() + "] resume() - creating resumeStage");
        CompletionStage<Void> resumeStage = phaseStage(this::resumeIterator, SuspendableActivity::resume, context, Functions.discardingBiConsumer());
        List<CompletionStage<Void>> phaseStages = List.of(resumeStage);
        // Resume activity groups in reverse priority order, i.e. last -> first
        CompletionStage<Void> result = resumeStage.whenComplete((ignore, exception) -> {
            System.err.println("[" + Thread.currentThread().getName() + "] resume.whenComplete() ENTER - state: " + this.state.get() + ", exception: " + exception);
            if (exception == null) {
                this.state.set(State.RUNNING);
                System.err.println("[" + Thread.currentThread().getName() + "] resume.whenComplete() - state set to RUNNING");
            }
            System.err.println("[" + Thread.currentThread().getName() + "] resume.whenComplete() EXIT - final state: " + this.state.get());
        });
        result.whenComplete(propagateCancellation(phaseStages));
        System.err.println("[" + Thread.currentThread().getName() + "] resume() EXIT - returning result future");
        return result;
    }

    private Iterator<List<SuspendableActivity>> resumeIterator() {
        return reverseIterator(this.activityGroups);
    }

    /**
     * Returns the stage for a suspend/resume phase.
     * @param <C> the stage context type
     * @param activityGroups the activity groups in a given iteration order
     * @param phase a function for this phase.
     * @param context the phase context
     * @return a completion stage for this phase of the suspend/resume process
     */
    private static <C> CompletionStage<Void> phaseStage(Iterable<List<SuspendableActivity>> activityGroups, BiFunction<SuspendableActivity, C, CompletionStage<Void>> phase, C context, BiConsumer<Void, Throwable> completionHandler) {
        System.err.println("[" + Thread.currentThread().getName() + "] phaseStage() ENTER");
        // Final stage will complete after all activity for all groups has completed
        CompletableFuture<Void> result = new CompletableFuture<>();
        // Make sure to register completion handler before initiating group completer
        result.whenComplete((v, ex) -> {
            System.err.println("[" + Thread.currentThread().getName() + "] phaseStage result.whenComplete() - exception: " + ex);
            try {
                completionHandler.accept(v, ex);
            } catch (Throwable t) {
                System.err.println("[" + Thread.currentThread().getName() + "] phaseStage completionHandler threw exception: " + t);
                t.printStackTrace(System.err);
                throw t;
            }
        });
        // Collect stages in case we need to cancel them
        List<CompletionStage<Void>> groupStages = new LinkedList<>();
        result.whenComplete(propagateCancellation(groupStages));
        // Iterate over activity groups (in the order dictated by the caller)
        Iterator<List<SuspendableActivity>> groups = activityGroups.iterator();
        new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void ignore, Throwable exception) {
                System.err.println("[" + Thread.currentThread().getName() + "] phaseStage groupCompleter.accept() - exception: " + exception);
                if (exception != null) {
                    System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - completing result exceptionally");
                    exception.printStackTrace(System.err);
                    result.completeExceptionally(exception);
                } else if (!groups.hasNext()) {
                    // No more groups
                    System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - no more groups, completing result");
                    result.complete(null);
                } else {
                    // Create stage for next group
                    List<SuspendableActivity> activities = List.copyOf(groups.next());
                    System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - processing next group with " + activities.size() + " activities");
                    CompletableFuture<Void> groupStage = new CompletableFuture<>();
                    groupStages.add(groupStage);
                    // Reuse groupCompleter instance as completion handler
                    groupStage.whenComplete(this);
                    if (activities.isEmpty()) {
                        // No activities, complete immediately
                        System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - group is empty, completing immediately");
                        groupStage.complete(null);
                    } else {
                        // Collect stages in case we need to cancel them
                        List<CompletionStage<Void>> stages = new ArrayList<>(activities.size());
                        groupStage.whenComplete(propagateCancellation(stages));
                        // Counter used to determine when all activities have complete
                        AtomicInteger activityCounter = new AtomicInteger(activities.size());
                        for (SuspendableActivity activity : activities) {
                            BiConsumer<Void, Throwable> activityCompleter = new BiConsumer<>() {
                                @Override
                                public void accept(Void ignore, Throwable exception) {
                                    System.err.println("[" + Thread.currentThread().getName() + "] phaseStage activityCompleter.accept() - exception: " + exception + ", remaining: " + activityCounter.get());
                                    if (exception != null) {
                                        System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - activity failed, completing group exceptionally");
                                        exception.printStackTrace(System.err);
                                        groupStage.completeExceptionally(exception);
                                    } else if (activityCounter.decrementAndGet() == 0) {
                                        // All activities of group have completed
                                        System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - all activities in group completed");
                                        groupStage.complete(null);
                                    }
                                }
                            };
                            try {
                                System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - calling phase.apply() for activity");
                                CompletionStage<Void> stage = phase.apply(activity, context);
                                stages.add(stage);
                                stage.whenComplete(activityCompleter);
                            } catch (Throwable e) {
                                System.err.println("[" + Thread.currentThread().getName() + "] phaseStage - phase.apply() threw exception: " + e);
                                e.printStackTrace(System.err);
                                activityCompleter.accept(null, e);
                            }
                        }
                    }
                }
            }
        }.accept(null, null);
        System.err.println("[" + Thread.currentThread().getName() + "] phaseStage() EXIT - returning result");
        return result;
    }

    static BiConsumer<Void, Throwable> propagateCancellation(List<CompletionStage<Void>> stages) {
        return new BiConsumer<>() {
            @Override
            public void accept(Void result, Throwable exception) {
                if (exception instanceof CancellationException) {
                    System.err.println("[" + Thread.currentThread().getName() + "] propagateCancellation - cancelling " + stages.size() + " stages");
                    for (CompletionStage<Void> stage : stages) {
                        stage.toCompletableFuture().cancel(false);
                    }
                } else if (exception != null) {
                    System.err.println("[" + Thread.currentThread().getName() + "] propagateCancellation - non-cancellation exception: " + exception);
                    exception.printStackTrace(System.err);
                }
            }
        };
    }

    /**
     * @deprecated Superseded by {@link #resume(ServerResumeContext)}.
     */
    @Deprecated(forRemoval = true)
    public void nonGracefulStart() {
        this.resume(Context.STARTUP).toCompletableFuture().join();
    }

    /**
     * @deprecated Superseded by {@link #resume(ServerResumeContext)}.
     */
    @Deprecated(forRemoval = true)
    public void resume() {
        System.err.println("[" + Thread.currentThread().getName() + "] resume() DEPRECATED - calling async resume()");
        try {
            CompletableFuture<Void> future = this.resume(Context.RUNNING).toCompletableFuture();
            System.err.println("[" + Thread.currentThread().getName() + "] resume() DEPRECATED - calling join()");
            future.join();
            System.err.println("[" + Thread.currentThread().getName() + "] resume() DEPRECATED - join() returned, state: " + this.state.get());
        } catch (Throwable t) {
            System.err.println("[" + Thread.currentThread().getName() + "] resume() DEPRECATED - exception caught: " + t);
            t.printStackTrace(System.err);
            throw t;
        }
    }

    /**
     * @deprecated Superseded by {@link #suspend(ServerSuspendContext)} using {@link CompletableFuture#completeOnTimeout(Object, long, TimeUnit)}.
     */
    @Deprecated(forRemoval = true)
    public void suspend(long timeoutMillis) {
        System.err.println("[" + Thread.currentThread().getName() + "] suspend(" + timeoutMillis + ") DEPRECATED - calling async suspend()");
        try {
            ServerLogger.ROOT_LOGGER.suspendingServer(timeoutMillis, TimeUnit.MILLISECONDS);
            CompletableFuture<Void> suspend = this.suspend(Context.RUNNING).toCompletableFuture();
            if (timeoutMillis >= 0) {
                suspend.completeOnTimeout(null, timeoutMillis, TimeUnit.MILLISECONDS);
            }
            System.err.println("[" + Thread.currentThread().getName() + "] suspend(" + timeoutMillis + ") DEPRECATED - calling join()");
            suspend.join();
            System.err.println("[" + Thread.currentThread().getName() + "] suspend(" + timeoutMillis + ") DEPRECATED - join() returned, state: " + this.state.get());
        } catch (Throwable t) {
            System.err.println("[" + Thread.currentThread().getName() + "] suspend(" + timeoutMillis + ") DEPRECATED - exception caught: " + t);
            t.printStackTrace(System.err);
            throw t;
        }
    }

    private static <E> Iterator<E> reverseIterator(List<E> list) {
        ListIterator<E> iterator = list.listIterator(list.size());
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasPrevious();
            }

            @Override
            public E next() {
                return iterator.previous();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public void forEachRemaining(Consumer<? super E> action) {
                while (this.hasNext()) {
                    action.accept(this.next());
                }
            }
        };
    }

    @Override
    public void registerActivity(SuspendableActivity activity, SuspendPriority priority) {
        Assert.checkNotNullParam("activity", activity);
        Assert.checkNotNullParam("priority", priority);
        if ((priority.ordinal() < SuspendPriority.FIRST.ordinal()) || (priority.ordinal() > SuspendPriority.LAST.ordinal())) {
            throw new IllegalArgumentException(String.valueOf(priority.ordinal()));
        }
        if (this.priorities.putIfAbsent(activity, priority) == null) {
            this.activityGroups.get(priority.ordinal()).add(activity);
            if (this.state.get() != State.RUNNING) {
                // if the activity is added when we are not running we just immediately suspend it
                // this should only happen at boot, so there should be no outstanding requests anyway
                // note that this means there is no execution group grouping of these calls.
                activity.suspend(Context.STARTUP).toCompletableFuture().join();
            }
        }
    }

    @Override
    public void unregisterActivity(SuspendableActivity activity) {
        SuspendPriority priority = this.priorities.remove(activity);
        if (priority != null) {
            this.activityGroups.get(priority.ordinal()).remove(activity);
        }
    }

    @Override
    public State getState() {
        return this.state.get();
    }

    @Override
    public void addListener(OperationListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void removeListener(OperationListener listener) {
        this.listeners.remove(listener);
    }

    @Deprecated(forRemoval = true)
    public void setStartSuspended(boolean startSuspended) {
        this.reset();
    }

    /**
     * Registers the given {@link ServerActivity} with this controller
     * @param activity the activity. Cannot be {@code null}
     * @throws IllegalArgumentException if {@code activity} is {@code null} of if its
     *                                  {@link ServerActivity#getExecutionGroup() getExecutionGroup()} method
     *                                  returns a value outside of that method's documented legal range.
     * @deprecated Superseded by {@link #registerActivity(SuspendableActivity)}.
     */
    @Deprecated(forRemoval = true)
    public void registerActivity(final ServerActivity activity) {
        this.registerActivity(activity, SuspendPriority.of(activity.getExecutionGroup()));
    }

    /**
     * @deprecated Superseded by {@link #unregisterActivity(SuspendableActivity)}.
     */
    @Deprecated(forRemoval = true)
    public void unRegisterActivity(final ServerActivity activity) {
        this.unregisterActivity(activity);
    }
}

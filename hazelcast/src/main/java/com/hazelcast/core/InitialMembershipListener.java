package com.hazelcast.core;

/**
 * The InitializingMembershipListener is a {@link MembershipListener} that will also receive a
 * {@link InitialMembershipEvent} when it is registered so it immediately knows which members are available.
 *
 * When the InitializingMembershipListener already is registered on a {@link Cluster} and is registered again on the same
 * Cluster instance, it will not receive an additional MembershipInitializeEvent. So this is a once only event.
 *
 * @author Peter Veentjer.
 * @see Cluster#addMembershipListener(MembershipListener)
 */
public interface InitialMembershipListener extends MembershipListener {

    /**
     * Is called when this listener is registered.
     *
     * @param event the MembershipInitializeEvent
     */
    void init(InitialMembershipEvent event);
}

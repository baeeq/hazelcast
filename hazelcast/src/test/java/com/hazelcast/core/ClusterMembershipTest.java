package com.hazelcast.core;

import org.junit.Before;
import org.junit.Test;

public class ClusterMembershipTest {

    @Before
    public void before() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        MembershipListenerImpl listener = new MembershipListenerImpl();
        Cluster cluster = hz.getCluster();
        cluster.addMembershipListener(listener);

        //create a new hzInstance and should trigger the listener
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        hz2.shutdown();
    }

    private static class MembershipListenerImpl implements MembershipListener {
        public void memberAdded(MembershipEvent e) {
            System.out.println(e);
        }

        public void memberRemoved(MembershipEvent e) {
            System.out.println(e);
        }
    }
}

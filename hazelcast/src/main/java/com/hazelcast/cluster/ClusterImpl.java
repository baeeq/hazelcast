/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cluster;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterImpl implements Cluster {

    final ConcurrentMap<MembershipListener, MembershipListenerActor> listeners = new ConcurrentHashMap<MembershipListener,MembershipListenerActor>();
    final AtomicReference<Set<Member>> members = new AtomicReference<Set<Member>>();
    final AtomicReference<Member> localMember = new AtomicReference<Member>();
    final Map<Member, Member> clusterMembers = new ConcurrentHashMap<Member, Member>();
    final Map<Address, Member> mapMembers = new ConcurrentHashMap<Address, Member>();
    @SuppressWarnings("VolatileLongOrDoubleField")
    volatile long clusterTimeDiff = Long.MAX_VALUE;
    final Node node;

    //do we want to have a basic synchronization; which can lead to blocked method calls. Or do we want to have a bit
    //smarter mechanism where the requests are queued and just processed by a single thread. For the time being this
    //crude approach should do the trick.
    final Object memberChangeMutex = new Object();

    public ClusterImpl(Node node) {
        this.node = node;
        this.setMembers(Arrays.asList(node.getLocalMember()));
    }

    public  void reset() {
        synchronized (memberChangeMutex){
            mapMembers.clear();
            clusterMembers.clear();
            members.set(null);
            this.setMembers(Arrays.asList(node.getLocalMember()));
        }
    }

    public  void setMembers(List<MemberImpl> lsMembers) {
        synchronized (memberChangeMutex) {
            Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
            ArrayList<Runnable> notifications = new ArrayList<Runnable>();
            final Set<Member> afterEventMembers = Collections.unmodifiableSet(new HashSet(lsMembers));
            for (MemberImpl member : lsMembers) {
                if (member != null) {
                    final MemberImpl dummy = new MemberImpl(member.getAddress(), member.localMember(),
                            member.getNodeType(), member.getUuid());
                    Member clusterMember = clusterMembers.get(dummy);
                    if (clusterMember == null) {
                        clusterMember = dummy;
                        if (!listeners.isEmpty()) {
                            notifications.add(new Runnable() {
                                public void run() {
                                    MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
                                            dummy, MembershipEvent.MEMBER_ADDED, afterEventMembers);
                                    for (MembershipListener listener : listeners.values()) {
                                        listener.memberAdded(membershipEvent);
                                    }
                                }
                            });
                        }
                    }
                    if (clusterMember.localMember()) {
                        localMember.set(clusterMember);
                    }
                    setNew.add(clusterMember);
                }
            }
            if (!listeners.isEmpty()) {
                Set<Member> it = clusterMembers.keySet();
                // build a list of notifications but send them AFTER removal
                for (final Member member : it) {
                    if (!setNew.contains(member)) {
                        notifications.add(new Runnable() {
                            public void run() {
                                MembershipEvent membershipEvent = new MembershipEvent(ClusterImpl.this,
                                        member, MembershipEvent.MEMBER_REMOVED, afterEventMembers);
                                for (MembershipListener listener : listeners.values()) {
                                    listener.memberRemoved(membershipEvent);
                                }
                            }
                        });
                    }
                }
            }
            clusterMembers.clear();
            mapMembers.clear();
            for (Member member : setNew) {
                mapMembers.put(((MemberImpl) member).getAddress(), member);
                clusterMembers.put(member, member);
            }
            members.set(Collections.unmodifiableSet(setNew));
            // send notifications now
            for (Runnable notification : notifications) {
                //there is no need to run this on a different thread since they only thing that is going to be done here
                //is to place the event on the queue inside of the actor and schedule it for execution. So that should
                //be very quick.
                notification.run();
            }
        }
    }

    public  void addMembershipListener(MembershipListener listener) {
        synchronized (memberChangeMutex){
            listeners.put(listener, new MembershipListenerActor(listener));

            //We are now going to send a member added event for each member this cluster currently has to the listener.
            //This is needed to make sure that a listener will be initialized with a consistent set of members
            Set<Member> currentMembers = members.get();
            Set<Member> eventMembers = new HashSet<Member>();
            for(Member addedMember : members.get()){
                eventMembers.add(addedMember);
                MembershipEvent e = new MembershipEvent(this, addedMember, MembershipEvent.MEMBER_ADDED, new HashSet(eventMembers));
                listener.memberAdded(e);
            }
        }
    }

    public void removeMembershipListener(MembershipListener listener) {
        listeners.remove(listener);
    }

    public Member getLocalMember() {
        return localMember.get();
    }

    public Set<Member> getMembers() {
        return members.get();
    }

    public long getClusterTime() {
        return Clock.currentTimeMillis() + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        if (Math.abs(diff) < Math.abs(clusterTimeDiff)) {
            this.clusterTimeDiff = diff;
        }
    }

    public long getClusterTimeFor(long localTime) {
        return localTime + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public Member getMember(Address address) {
        return mapMembers.get(address);
    }

    @Override
    public String toString() {
        Set<Member> members = getMembers();
        StringBuffer sb = new StringBuffer("Cluster [");
        if (members != null) {
            sb.append(members.size());
            sb.append("] {");
            for (Member member : members) {
                sb.append("\n\t").append(member);
            }
        }
        sb.append("\n}\n");
        return sb.toString();
    }

    private class MembershipListenerActor implements MembershipListener, Runnable{
        private final BlockingQueue<MembershipEvent> mailbox = new LinkedBlockingQueue<MembershipEvent>();
        private final MembershipListener target;
        private final AtomicBoolean scheduled = new AtomicBoolean(false);

        private MembershipListenerActor(MembershipListener target) {
            this.target = target;
        }

        public void memberAdded(MembershipEvent e) {
            mailbox.add(e);
            ensureScheduled();
        }

        public void memberRemoved(MembershipEvent e) {
            mailbox.add(e);
            ensureScheduled();
        }

        private void ensureScheduled() {
            if(scheduled.get()){
                //it is already scheduled, so we are finished.
                return;
            }

            if(!scheduled.compareAndSet(false, true)){
                //somebody else scheduled it, so we are finished.
                return;
            }

            //we manage to schedule it, lets hand it over to an executor so it can be processed.
            node.executorManager.getEventExecutorService().execute(this);
        }

        public void run() {
            try {
                MembershipEvent e = mailbox.poll();
                switch (e.getEventType()) {
                    case MembershipEvent.MEMBER_ADDED:
                        target.memberAdded(e);
                        break;
                    case MembershipEvent.MEMBER_REMOVED:
                        target.memberRemoved(e);
                        break;
                    default:
                        throw new RuntimeException("Unhandeled event: " + e);
                }
            } finally {
                scheduled.set(false);
                if (!mailbox.isEmpty()) {
                    //if the mailbox has items, we need to ensure that it scheduled so that no unprocessed messages
                    //remain in the mailbox.

                    //if performance, so processing a zillion events a second, would be a concern, then you probably want
                    //to 'batch' the processing of the events; so drain a whole number of events from the mailbox to be processed.
                    //But members are not being added/removed in that frequency, so it don't need to increase complexity
                    //to improve performance.
                    ensureScheduled();
                }
            }
        }
    }
}

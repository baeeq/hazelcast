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

import com.hazelcast.core.*;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterImpl implements Cluster {

    final CopyOnWriteArraySet<MembershipListener> listeners = new CopyOnWriteArraySet<MembershipListener>();
    final AtomicReference<Set<Member>> members = new AtomicReference<Set<Member>>();
    final AtomicReference<Member> localMember = new AtomicReference<Member>();
    final Map<Member, Member> clusterMembers = new ConcurrentHashMap<Member, Member>();
    final Map<Address, Member> mapMembers = new ConcurrentHashMap<Address, Member>();
    @SuppressWarnings("VolatileLongOrDoubleField")
    volatile long clusterTimeDiff = Long.MAX_VALUE;
    final Node node;
    final Object memberChangeMutex = new Object();

    public ClusterImpl(Node node) {
        this.node = node;
        this.setMembers(Arrays.asList(node.getLocalMember()));
    }

    public void reset() {
        mapMembers.clear();
        clusterMembers.clear();

        synchronized (memberChangeMutex){
            //todo: do we want a null set or do we want an empty set? Now we are at the risk of exposing null
            //to the end user.
            members.set(null);
            this.setMembers(Arrays.asList(node.getLocalMember()));
        }
    }

    public void setMembers(List<MemberImpl> lsMembers) {
        Set<Member> setNew = new LinkedHashSet<Member>(lsMembers.size());
        Set<Member> membersAfterEvent = new LinkedHashSet<Member>(lsMembers);

        List<Notification> notifications = new LinkedList<Notification>();

        synchronized (memberChangeMutex) {

            //check if any members have been added
            for (MemberImpl member : lsMembers) {
                final MemberImpl dummy = new MemberImpl(member.getAddress(), member.localMember(),
                        member.getNodeType(), member.getUuid());
                Member clusterMember = clusterMembers.get(dummy);
                if (clusterMember == null) {
                    clusterMember = dummy;
                    MembershipEvent event = new MembershipEvent(this, dummy, MembershipEvent.MEMBER_ADDED, membersAfterEvent);
                    for (MembershipListener listener : listeners) {
                        notifications.add(new Notification(event, listener));
                    }
                }

                if (clusterMember.localMember()) {
                    localMember.set(clusterMember);
                }
                setNew.add(clusterMember);
            }

            //check if any members have been removed.
            if (listeners.size() > 0) {
                // build a list of notifications but send them AFTER removal
                for ( Member member : clusterMembers.keySet()) {
                    if (!setNew.contains(member)) {
                        MembershipEvent event = new MembershipEvent(this, member, MembershipEvent.MEMBER_REMOVED, membersAfterEvent);
                        for (MembershipListener listener : listeners) {
                            notifications.add(new Notification(event, listener));
                        }
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
            for (Notification notification : notifications) {
                node.executorManager.getEventExecutorService().executeOrderedRunnable(notification.listener.hashCode(), notification);
            }
        }
    }

    public void addMembershipListener(MembershipListener listener) {
        if(!(listener instanceof InitialMembershipListener)){
            listeners.add(listener);
        }else{
            synchronized (memberChangeMutex) {
                if(!listeners.add(listener)){
                    //the listener is already registered, so we are not going to send the initialize event.
                    return;
                }

                final InitialMembershipListener initializingListener = (InitialMembershipListener) listener;
                final InitialMembershipEvent event = new InitialMembershipEvent(this, members.get());

                node.executorManager.getEventExecutorService().executeOrderedRunnable(listener.hashCode(), new Runnable(){
                    public void run() {
                        initializingListener.init(event);
                    }
                });
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

    private static class Notification implements Runnable{
        private final MembershipListener listener;
        private final MembershipEvent event;

        private Notification(MembershipEvent event, MembershipListener listener) {
            this.event = event;
            this.listener = listener;
        }

        public void run() {
            switch (event.getEventType()) {
                case MembershipEvent.MEMBER_ADDED:
                    listener.memberAdded(event);
                    break;
                case MembershipEvent.MEMBER_REMOVED:
                    listener.memberRemoved(event);
                    break;
                default:
                    throw new RuntimeException("Unhandeled event: " + event);
            }
        }
    }
}

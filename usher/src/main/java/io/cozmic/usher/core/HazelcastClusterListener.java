package io.cozmic.usher.core;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;

import com.hazelcast.client.HazelcastClient;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chuck on 10/24/14.
 */
public class HazelcastClusterListener implements ClusterListener {
    private final ClientConfig config;
    private Listener listener;
    private HazelcastInstance hzl;

    public HazelcastClusterListener(ClientConfig clientConfig) {
        config = clientConfig;
        hzl = HazelcastClient.newHazelcastClient(config);
    }

    @Override
    public ClusterListener listener(Listener listener) {
        this.listener = listener;
        return this;
    }

    @Override
    public void start() {



        hzl.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                addMember(membershipEvent.getMember());
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                listener.removed(membershipEvent.getMember().getUuid());
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

            }
        });

    }

    @Override
    public Map<String, InetSocketAddress> getMembers() {
        final ConcurrentHashMap<String, InetSocketAddress> members = new ConcurrentHashMap<>();
        for (Member member : hzl.getCluster().getMembers()) {
            members.put(member.getUuid(), member.getSocketAddress());
        }
        return members;
    }



    private void addMember(Member member) {
        final InetSocketAddress socketAddress = member.getSocketAddress();
        listener.added(member.getUuid(), socketAddress.getHostName(), socketAddress.getPort());
    }
}

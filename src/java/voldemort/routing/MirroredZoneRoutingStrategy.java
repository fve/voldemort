/*
 * Routing strategy using a special hash to split partitions in groups of 3 partitions (one in each zone).
 * 
 * 1231231234123123123123...
 * 
 * The hash always hits a "1" partition, then we walk to the partition in our zone.
 * 
 * Partition distribution uses specific tools to build the list of partitions per node and per disk inside a node
 * 
 * Copyright 2012 EZC Group S.A.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * 
 * Based on ZoneRoutingStrategy, Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import voldemort.cluster.Node;
import voldemort.utils.FnvForMirroredZonesHashFunction;
import voldemort.utils.FnvHashFunction;
import voldemort.utils.HashFunction;

/**
 * A Zone Routing strategy that sits on top of the Consistent Routing strategy
 * such that each request goes to zone specific replicas
 * 
 */
public class MirroredZoneRoutingStrategy extends ConsistentRoutingStrategy {

    private HashMap<Integer, Integer> zoneReplicationFactor;
    private final HashFunction mirrorHash;

    public MirroredZoneRoutingStrategy(Collection<Node> nodes,
                               HashMap<Integer, Integer> zoneReplicationFactor,
                               int numReplicas) {
        super(null, nodes, numReplicas); // Hash will directly be used in this class because we need to reduce to multiple of tree after reducing to number of partitions
        this.zoneReplicationFactor = zoneReplicationFactor;
        mirrorHash = new FnvForMirroredZonesHashFunction(getPartitionToNode().length/3) ;
    }


    /**
     * Get the replication partitions list for the given partition.
     * 
     * @param index Partition id for which we are generating the preference list
     * @return The List of partitionId where this partition is replicated.
     */
    @Override
    public List<Integer> getReplicatingPartitionList(int index) {
        List<Node> preferenceNodesList = new ArrayList<Node>(getNumReplicas());
        List<Integer> replicationPartitionsList = new ArrayList<Integer>(getNumReplicas());

        // Copy Zone based Replication Factor
        HashMap<Integer, Integer> requiredRepFactor = new HashMap<Integer, Integer>();
        requiredRepFactor.putAll(zoneReplicationFactor);

        // Cross-check if individual zone replication factor equals 1, and that we have 3 zones
        int sum = 0;
        for(Integer zoneRepFactor: requiredRepFactor.values()) {
        	if ( zoneRepFactor != 1 )
        		throw new IllegalArgumentException("Each zone must have ONE replica with mirrored-zone-routing");
            sum += zoneRepFactor;
        }

        if(sum != getNumReplicas())
            throw new IllegalArgumentException("Number of zone replicas is not equal to the total replication factor");
        
        if (sum != 3)
            throw new IllegalArgumentException("Number of zones must be 3 with mirrored-zone-routing");

        if(getPartitionToNode().length == 0) {
            return new ArrayList<Integer>(0);
        }

        for(int i = 0; i < getPartitionToNode().length; i++) {
            // add this one if we haven't already
            Node currentNode = getNodeByPartition(index);
            if(!preferenceNodesList.contains(currentNode)) {
                preferenceNodesList.add(currentNode);
                if(checkZoneRequirement(requiredRepFactor, currentNode.getZoneId()))
                    replicationPartitionsList.add(index);
            }

            // if we have enough, go home
            if(replicationPartitionsList.size() >= getNumReplicas())
                return replicationPartitionsList;
            // move to next clockwise slot on the ring
            index = (index + 1) % getPartitionToNode().length;
        }

        // we don't have enough, but that may be okay
        return replicationPartitionsList;
    }

    private boolean checkZoneRequirement(HashMap<Integer, Integer> requiredRepFactor, int zoneId) {
        if(requiredRepFactor.containsKey(zoneId)) {
            if(requiredRepFactor.get(zoneId) == 0) {
                return false;
            } else {
                requiredRepFactor.put(zoneId, requiredRepFactor.get(zoneId) - 1);
                return true;
            }
        }
        return false;

    }

    @Override
    public String getType() {
        return RoutingStrategyType.ZONE_STRATEGY;
    }
    
    @Override
    public List<Integer> getPartitionList(byte[] key) {
        int index = mirrorHash.hash(key);
        return getReplicatingPartitionList(index);
    }
}

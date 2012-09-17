/* Special Hash function for managing disk to disk replicas ( N sets of 3 disks )
 * Get background at: http://www.yobidrive.com/blog/and-then-they-were-three-and-red-green-blue/
 * This function is also used by the log-forward, disk-based hashtable DiskMap KV Storage
 * 
 * 
 * 
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
 * Based on FNVHashFunction
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.utils;

/**
 * Taken from http://www.isthe.com/chongo/tech/comp/fnv
 * 
 * hash = basis for each octet_of_data to be hashed hash = hash * FNV_prime hash
 * = hash xor octet_of_data return hash
 * 
 * 
 */
public class FnvForMirroredZonesHashFunction implements HashFunction {

    private static final long FNV_BASIS = 0x811c9dc5;
    private static final long FNV_PRIME = (1 << 24) + 0x193;
    public static final long FNV_BASIS_64 = 0xCBF29CE484222325L;
    public static final long FNV_PRIME_64 = 1099511628211L;
    private int nbMasterPartitions = 2048 ;

    
    public FnvForMirroredZonesHashFunction(int nbMasterPartitions) {
    	this.nbMasterPartitions = nbMasterPartitions ;
    }

    public int hash(byte[] key) {
        long hash = FNV_BASIS;
        for(int i = 0; i < key.length; i++) {
            hash ^= 0xFF & key[i];
            hash *= FNV_PRIME;
        }
        // Hash is a 32 bits integer 
        return ((abs((int)  hash)) % nbMasterPartitions) * 3;
    }
    
    private  int abs(int a) {
        if(a >= 0)
            return a;
        else if(a != Integer.MIN_VALUE)
            return -a;
        return Integer.MAX_VALUE;
    }

    

    public static void main(String[] args) {
        if(args.length != 2)
            Utils.croak("USAGE: java FnvHashFunction iterations buckets");
        int numIterations = Integer.parseInt(args[0]);
        int numBuckets = Integer.parseInt(args[1]);
        int[] buckets = new int[numBuckets];
        HashFunction hash = new FnvForMirroredZonesHashFunction(2048);
        for(int i = 0; i < numIterations; i++) {
            int val = hash.hash(Integer.toString(i).getBytes());
            buckets[Math.abs(val) % numBuckets] += 1;
        }

        double expected = numIterations / (double) numBuckets;
        for(int i = 0; i < numBuckets; i++)
            System.out.println(i + " " + buckets[i] + " " + (buckets[i] / expected));
    }

}

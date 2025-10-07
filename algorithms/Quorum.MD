# Quorum in Distributed Systems

## Basic Concepts

- **N** – Total number of nodes in our system
- **W** – Minimum number of nodes required for a successful write operation
- **R** – Minimum number of nodes required for a successful read operation

To introduce the concept of Quorum, for an N node system, we must confirm a write as successful if it is confirmed by at least W replicas and read operation to be successful if confirmed by at least R replicas. Another important property required for a successful quorum is:

> **R + W > N**

## Example

If our system has 10 total nodes (N=10), then if we set W=3 and R=3 then we can have a successful write operation if 3 nodes confirm the write and a successful read operation if 3 nodes confirm the read. But this configuration does not satisfy the quorum property as R + W = 6 which is not greater than N=10. So this configuration is not valid. This makes the system not fault tolerant as it can lead to scenarios where a read operation does not return the latest write.

So a valid configuration can be W=6 and R=5. This means that a write operation is successful if 6 nodes confirm the write and a read operation is successful if 5 nodes confirm the read. This configuration satisfies the quorum property as R + W = 11 which is greater than N=10. This makes the system fault tolerant as it can lead to scenarios where a read operation always returns the latest write.

## Impact of R & W Values

Changing the value of R & W impacts the latency of reads & writes of our application. For a read heavy system with strong consistency requirements, we can set W = N & R = 1. So read requests will be processed quickly and we will always get consistent results as we have already updated the resource on all N nodes. But our writes will be blocked by failure of even a single node. Whereas for a write heavy system, we can go to the other extreme and set W = 1. But doing so we end up in a scenario of data loss if a node goes down and the resource has not yet been replicated to any other node.

Hence it requires studying the traffic metrics and finding the right balance between both the values. Decreasing the value of R & W increases the number of node failures our application can tolerate. This opens up possibilities for making our system highly available if we can compromise on consistency:

1. If we are ok with data loss in our system then we can lower the value of W to 1 which will end up making our application highly available for write-heavy scenarios.
2. If clients of our application are ok with eventual consistency and can operate even after reading stale results (e.g., number of likes on a post) then we can decrease the value of R to 1 which will make our application robust for read-heavy scenarios.

## Limitations of Quorum

By satisfying the above property for a quorum we can cover the majority of scenarios in a leaderless system. Though there are certain edge cases which are hard to tackle if we have strict requirements for consistency:

- Quorums still cannot resolve the issue with concurrent writes. In case a resource is updated simultaneously by two clients then we cannot decide which operation happened first. There are various approaches to handle conflict resolution but all of them either result in data loss or moving the responsibility of resolving the conflict to the application layer.
- If there is a read request simultaneously with a write request then we cannot be sure that the Quorum will return the most updated value. As the write and read are happening concurrently, it can be the case that replicas where the write has replicated until now are not part of read Quorum.

## Sloppy Quorum and Hinted Handoff: Quorum in the Times of Failure

As discussed earlier, Quorums play a huge part in multi-node systems. However, even though the criteria of R + W > N looks achievable at first glance, we will start seeing its limitations once our system is deployed on more than one cluster of nodes or once we start seeing frequent node failures in our system. Quorums are not fault tolerant and we can easily end up in a scenario where the nodes from one cluster are unable to communicate to nodes from another cluster. This will result in failure to reach a quorum and result in failing read and write requests from the client.

Consider a scenario where we have assigned N nodes for a particular key. These N nodes are spread across two clusters and none of these clusters have more than W - 1 nodes for this key. Whenever we receive a request from the client to update the key, we send it to these N nodes and if we receive a confirmation from W of these N nodes, we consider the update to be successful. Now if we encounter a network failure between these two node clusters then we won't be able to reach a quorum for all the upcoming write requests as we are unable to communicate across all N nodes and gather at least W votes to get a quorum.

To handle the above issue we have a modified version of quorum called as **Sloppy Quorum**. Under sloppy quorum, when we are unable to reach all the N nodes due to a partition failure, we temporarily store the updates on backup nodes. These backup nodes were not initially responsible for storing updates for the required key but they store the updates only in case of partition failure. The updates are stored along with the metadata which describes the original node that the key was required to be stored on.

So let us now revisit the above example. We had N nodes spread across two clusters P1 & P2. Now we are unable to reach P2 cluster so in order to save an update for a key, we will go through the following flow:

1. Store the update on original nodes for the key on P1 cluster
2. Store update on backup nodes present on P1 cluster. Each backup node maps to the original node on P2 cluster. The update will contain the details about which node the update originally belongs to on P2 cluster

With the above flow, we are able to reach a quorum and process the write request for the client. Each backup node will ping the original node on P2 cluster to check if partition failure is resolved. Once the backup node is able to communicate, it will share the update with original node on P2 cluster and remove the update from its own storage. This process of sharing the update post failure resolution is called **hinted handoff**.

### In Simple Words

A real-life example of sloppy quorum & hinted handoff will be if a coworker takes a message on your behalf once you are out on a break and shares the message with you once you are back. If you didn't have such an understanding coworker with you then you might have missed the message completely. You will have to figure out on your own about what messages you missed and will be scared to leave your desk in future for a break.

## Conclusion

So why not always use the updated variation of sloppy quorum and hinted handoff instead of boring old quorum? While this updated variation is perfect for a write-heavy system, it might not be the best solution if your system requires consistent reads. During network failure, your system will mark a write operation as successful by using sloppy quorum but note that not all the nodes for this updated record are in sync with the most recent update. So even though clusters in your system are unable to communicate amongst each other, a client might be able to send a read request to an outdated cluster and will receive a stale value. Systems that can tolerate eventually consistent results are the best candidates for this methodology. One example of such system is Dynamo which leverages sloppy quorum and hinted handoff to overcome temporary node and partition failure.

As an application developer, you will have to make the call about using sloppy quorum over strict quorum based on what are the requirements for your system. If you have strict requirements on showing the most consistent state to end-user then stick to strict quorums and ask the client to retry in case of node/network failures. But if you are ok with eventually consistent results then you can start exploring the combination of sloppy quorum and hinted handoff to build a highly available system.

# Vector Clocks: Keeping Time in Check

*Posted on May 1, 2022 by varunu28*

> "Lost time is never found again." – Benjamin Franklin.

The above quote is true to its core in context of distributed systems. If you miss out on keeping track of events in a cluster of 100+ machines then good luck building a system that acts as a source of truth.

## Introduction

Time is an essential component when dealing with events in an application. Time is used to answer questions such as:

- When did this event occurred?
- Out of these two events, which one occurred first?
- Out of these n events, which event occurred last?
- What events occurred simultaneously?

In case of a single node application, the above questions can easily be answered by referencing the system clock. Whenever an event occurs you can record the current time and use this recorded time to compare with events that will occur in future to build a timeline. You can easily answer the above questions by comparing the recorded time and be sure that you have the right answer as there is only one source of time i.e. your system clock.

## The Problem with Distributed Systems

But this guarantee is no longer there once you move to a multi-node system. Due to **clock skew**, we can no longer be sure that system clocks on two different nodes yield the same time. Each clock can run at different rates and we can no longer reference system clock time for comparing the ordering of events occurring on two systems. 

So if Server_A writes Z=1 at time T1 and Server_B writes Z=2 at time T2, then we cannot ensure that Server_A wrote before Server_B even though T1 < T2. This turns out to be a major problem when dealing with events occurring in distributed systems with network partitions. You need a mechanism to find the correct ordering of events in order to ensure that system eventually reconciles to an agreeable state.

## Vector Clocks

**Vector clock** is a data structure that achieves the goal of determining the causality of events occurring on different servers. The vector clock maintains versions of events on each server in form of an array. Considering we have a system of 3 nodes, we will have an integer array of size 3 where each entry is initialized to zero i.e. `{0,0,0}`. This array is tied to every event (For example set key Z=1 is an event) occurring on each node.

Let's dive into an example to understand how vector clock is used to determine if two events are concurrent or an event follows another event.

```
Server1             Server2             Server3
{0,0,0}             {0,0,0}             {0,0,0}
   |                   |                   |
   |--E1 {1,0,0}       |                   |     # 1. Server1 increments its counter
   |                   |                   |
   |                   |--E1 {0,1,0}       |     # 2. Server2 increments its counter
   |                   |                   |
   |--E2 {2,0,0}       |                   |     # 3. Server1 increments again
   |                   |                   |
   |                   |                   |--E1 {0,0,1}  # 4. Server3 increments its counter
   |                   |                   |
   |                   |                   |
   |----E3 {3,0,0}---->|                   |     # 5. Server1 sends event to Server2
   |                   |--E2 {3,2,0}       |     #    Server2 syncs clock
   |                   |                   |
   |                   |                   |
   |                   |<----E2 {0,0,2}----|     # 6. Server3 sends event to Server2
   |                   |--E3 {3,3,2}       |     #    Server2 syncs clock again
```

In the above example, we have three servers generating events. Initially all the three servers start with a vector clock initialized to `{0, 0, 0}`. Let's start looking into how each event is handled on the individual servers:

1. Event E1 is generated on Server1. Server1 records the event by incrementing the vector clock by one at the index mapping to Server1 i.e. first index. This results in Server1 vector clock to become `{1, 0, 0}`
2. Event E1 is generated on Server2. Server2 increments its vector clock similar to Server1 on its index making the vector clock of Server2 `{0, 1, 0}`.
3. Event E2 is generated on Server1. Server1 till now doesn't have any information of events occurring on other servers. It moves forward by incrementing its vector clock to `{2, 0, 0}`
4. Event E1 is generated on Server3 and it updates the vector clock in a similar manner to `{0, 0, 1}`.
5. Now Server1 sends an event to Server2. It sends an updated vector clock i.e. `{3, 0, 0}` as part of the request. Server2 acknowledges that this event is not generated on its server but rather an event coming from Server1. It needs to now perform a clock synchronization which is done by picking the maximum value for each servers. So Server2 first increments its own index to record the event and then synchronizes the vector clock making it `{3, 2, 0}` for event E2.
6. Server3 similar to Server1 sends an event to Server2 with updated clock i.e. `{0, 0, 2}`. Server2 performs another clock synchronization making clock for event E3 on Server2 as `{3, 3, 2}`.

Above process is followed to synchronize clocks on servers. Once we have this synchronization in place, we can reference the vector clock of events occurring on each of these servers to find the causality of events.

## Determining Event Order

Rules to figure out ordering of two events based on their vector clock (V1 & V2) is as follows:

1. If majority of elements in V1 are less than or equal to V2 and at least one element of V1 is less than element of V2 then we can say that V1 is causal to V2 i.e. event associated with V1 occurs before event associated with V2.
   - More formally it can be expressed as V1 <= V2 along with V1[i] < V2[j] for i = 1 to n.
   - Finding this "happens before" relationship makes reconciliation of events easier in the system. If user sets A=1 on Server1 as event E1 and sets A=2 on Server2 as event E2 then we can compare the vector clocks of these two events to figure out the ordering of events.

2. Consider a simple case of event E1 occurring on Server1 with V1 as `{1, 0, 0}` and event E2 occurring on Server1 with V2 as `{2, 0, 0}`. Here majority (2 out of 3) of elements in V1 is less than or equal to elements of V2. So we can say that V1 is causal to V2.

3. For a more complex case, consider event E2 on Server1 with V1 as `{2, 0, 0}` and event E2 on Server2 with V2 as `{3, 2, 0}`. Comparing V1 & V2 we can figure out that V1 occurs before V2.

4. If we cannot reach a conclusion from above comparison then we can say that two events are concurrent.
   - If neither (V1 <= V2 & NOT(V2 <= V1)) nor (V2 <= V1 & NOT(V1 <= V2))
   - In this case a common approach for reconciliation is to save both the events and shift the responsibility of reconciliation to client.

5. Consider event E1 on Server2 with vector clock V1 `{1, 0, 0}` & event E1 on Server3 with vector clock V2 `{0, 0, 1}`. Here neither V1 has all elements less than or equal to V2 nor V2 has all elements less than or equal to V1.

## Real-World Application

One of the famous real-life use case of vector clock is how **DynamoDB** uses it to maintain ordering of events. Dynamo is built to tolerate network partitions and it continues accepting writes even in case the nodes are unable to communicate across the partitions. This can lead to an object ending up in different states across two nodes. So for reconciliation, it uses vector clock of events associated with these objects to reach a final state.

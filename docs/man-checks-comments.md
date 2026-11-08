# Comments to manual check

Why I'm seeing this in log after group foundation:
2026-11-07T10:54:31.539847Z  WARN A message will be ignored because vote changed: msg sent by vote: T1-Ncde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed:uncommitted; current my vote: None; when (VoteResponse)


UI - Tab name
it'll be useful to have node name in tab - Alice, Bob -  useful for testing

Uploading of 1GB - in UI
Seems to be bit slow - considering that everything is local

Custody log
After have couple of item globally in custody and having bob custody lectures - 1GB item, but his log shows:
2026-11-07T11:26:52.580968Z  INFO custody computed custody=CustodyStatus { items: 0, bytes: 0, pledge_bytes: 5000000000, load_factor: Some(0.0) }
WTF

Node pledge in UI
Enable rasing node pledge in UI - link to is if get error in Add

Chap. 5 - Dave's UI Library
I see there 4 items - erin's note is there -  but guide says 3 items -  that not clear - why first Erin file was added even when it was over his pledge ??? Message is not clear that it's only warning - should be more clear - like added now, but will have problem later!

Wishes UI is pathetic
Needs bit rework -   have existing wishes as table, button for new wish under it, new wish on separate page - WTF is comment on wishes???

Fails to send gossip on termination (sigint)
Seen some warning that cannot send gossips - due to abort -  I guess it's OK as it was not always, wondering if some important gossip need to go out on shutdown? 

Bob got into some evil loop when other nodes were kill
Bob was last survivor -  he started to spill a lot of warning and error messages - every second

```
2026-11-08T09:13:35.444280Z ERROR RPCError err=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.444326Z  WARN openraft::replication::ReplicationCore<_, _, _>::backoff_drain_events backoff mode: drain events without processing them interval=499.999345ms
2026-11-08T09:13:35.444379Z  WARN update progress error request_id=HeartBeat result=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.444388Z  WARN heartbeat error: Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping, no update to inflight data
2026-11-08T09:13:35.764087Z  WARN error replication to target=6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d error=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.764152Z ERROR RPCError err=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.764208Z  WARN openraft::replication::ReplicationCore<_, _, _>::backoff_drain_events backoff mode: drain events without processing them interval=499.999118ms
2026-11-08T09:13:35.764297Z  WARN update progress error request_id=HeartBeat result=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.764331Z  WARN heartbeat error: Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping, no update to inflight data
2026-11-08T09:13:35.947050Z  WARN error replication to target=cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed error=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.947100Z ERROR RPCError err=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.947162Z  WARN openraft::replication::ReplicationCore<_, _, _>::backoff_drain_events backoff mode: drain events without processing them interval=499.99936ms
2026-11-08T09:13:35.947220Z  WARN update progress error request_id=HeartBeat result=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:35.947249Z  WARN heartbeat error: Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping, no update to inflight data
2026-11-08T09:13:36.267283Z  WARN error replication to target=6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d error=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.267348Z ERROR RPCError err=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.267413Z  WARN openraft::replication::ReplicationCore<_, _, _>::backoff_drain_events backoff mode: drain events without processing them interval=499.999344ms
2026-11-08T09:13:36.267523Z  WARN update progress error request_id=HeartBeat result=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.267557Z  WARN heartbeat error: Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping, no update to inflight data
2026-11-08T09:13:36.449354Z  WARN error replication to target=cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed error=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.449422Z ERROR RPCError err=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.449491Z  WARN openraft::replication::ReplicationCore<_, _, _>::backoff_drain_events backoff mode: drain events without processing them interval=499.999259ms
2026-11-08T09:13:36.449553Z  WARN update progress error request_id=HeartBeat result=Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.449573Z  WARN heartbeat error: Unreachable node: distlib_net::error::NetError: communication with cde74476d0dbead6c39291b00046115426b42f5a9de57a5767cad69f0f7420ed failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping, no update to inflight data
2026-11-08T09:13:36.768990Z  WARN error replication to target=6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d error=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.769045Z ERROR RPCError err=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.769090Z  WARN openraft::replication::ReplicationCore<_, _, _>::backoff_drain_events backoff mode: drain events without processing them interval=499.999214ms
2026-11-08T09:13:36.769146Z  WARN update progress error request_id=HeartBeat result=Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping
2026-11-08T09:13:36.769175Z  WARN heartbeat error: Unreachable node: distlib_net::error::NetError: communication with 6d92d8596f1e3a4168fb973460d4d0064ac4194bf88ea2f4507503e4045b083d failed; source: Unable to connect to remote; source: Unable to connect to remote; source: endpoint stopping, no update to inflight data
```


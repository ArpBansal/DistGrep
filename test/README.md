# Test Suite Documentation

All tests are organized in the `test/` directory and follow Go's standard testing conventions.

## Test Files

### 1. `chaos_test.go` - MapReduce Chaos Testing
Tests the resilience of the MapReduce-based distributed grep system when components fail.

**Tests:**
- `TestReducerFailureRecovery`: Verifies system handles reducer failures with retries
- `TestMapperFailureRecovery`: Verifies system recovers when mappers fail
- `TestCombinedChaos`: Tests simultaneous failures in both mappers and reducers
- `BenchmarkReducerWithChaos`: Benchmarks performance under failure conditions

**Features:**
- Failure injection with configurable failure rates
- Automatic retry logic (up to 3 attempts)
- Comprehensive statistics on failures and recovery

### 2. `raft_test.go` - Raft Consensus Testing
Tests the Raft-based master consensus system for distributed coordination.

**Tests:**
- `TestRaftClusterConsensus`: Basic leader election in single-node cluster
- `TestRaftTaskReplication`: Task replication through Raft consensus
- `TestRaftWorkerRegistration`: Worker registration through replicated state
- `TestRaftWorkerHeartbeat`: Worker health updates and status tracking
- `TestMasterCoordinator`: End-to-end master coordinator workflow
- `TestRaftLeaderFailover`: Task persistence across leader elections
- `TestRaftConsistency`: Consistent state across cluster nodes

**Benchmarks:**
- `BenchmarkRaftTaskReplication`: Task replication throughput
- `BenchmarkMasterCoordinator`: Master coordinator operations throughput

## Running Tests

### Run all tests:
```bash
go test -v ./test/ -timeout 60s
```

### Run specific test:
```bash
go test -v ./test/ -run TestMasterCoordinator -timeout 10s
```

### Run only benchmarks:
```bash
go test -bench=. ./test/ -benchtime=10s
```

### Run chaos tests only:
```bash
go test -v ./test/ -run Chaos -timeout 30s
```

### Run Raft tests only:
```bash
go test -v ./test/ -run Raft -timeout 30s
```

## Test Architecture

### MapReduce Layer (chaos_test.go)
- **FailingReducer**: Wrapper that simulates reducer failures
- **FailingMapper**: Wrapper that simulates mapper failures
- Both implement automatic retry with exponential backoff

### Raft Consensus Layer (raft_test.go)
- **Single-node clusters**: For basic functionality testing
- **Stateless workers**: No replication needed on worker side
- **Replicated masters**: 3 or 5 master replicas for high availability

## Key Test Scenarios

1. **Chaos Resilience**: MapReduce continues despite 20-30% component failures
2. **Leader Election**: Automatic election in ~500ms
3. **State Replication**: All tasks and workers replicated across cluster
4. **Failover**: Task state persists across master restarts
5. **Consistency**: All cluster nodes maintain identical state

## Test Configuration

- **Raft Ports**: 5001-5009 (isolated for each test)
- **Heartbeat Timeout**: 200ms
- **Election Timeout**: 200ms
- **Leader Lease Timeout**: 100ms
- **Snapshot Interval**: 2 seconds
- **Temp Directories**: Each test uses isolated tmpdir

## Performance Benchmarks

Run with:
```bash
go test -bench=BenchmarkRaftTaskReplication ./test/ -benchtime=10s
go test -bench=BenchmarkMasterCoordinator ./test/ -benchtime=10s
go test -bench=BenchmarkReducerWithChaos ./test/ -benchtime=10s
```

## Debugging Tests

Enable Raft logs:
```bash
LOGLEVEL=DEBUG go test -v ./test/ -run TestMasterCoordinator
```

Check test timeout issues:
```bash
go test -v ./test/ -timeout 120s
```

## Test Isolation

- Each test uses a unique temp directory
- Each test uses unique port numbers
- No shared state between tests
- Tests can run in parallel: `go test -p 4 ./test/`

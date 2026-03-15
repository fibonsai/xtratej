# JMH Benchmarks for xtratej

This document provides instructions and best practices for running and maintaining the JMH (Java Microbenchmark Harness) benchmarks for the xtratej project.

## Overview

The xtratej project includes JMH benchmarks for measuring the performance of critical paths in the system. These benchmarks are essential for:
- Performance regression testing
- Optimization verification
- Performance characterization
- Monitoring critical path performance

## Benchmark Categories

### 1. Rule Evaluation Benchmarks (`RuleEvaluationBenchmark`)
- **Purpose**: Measure performance of rule evaluation operations (CrossedRule, LimitRule, TrendRule)
- **Key Metrics**: Average time for rule evaluation
- **Scenarios**: Simple rule evaluation, complex rule chains

### 2. FIFO Operations Benchmarks (`FifoZipBenchmark`)
- **Purpose**: Measure performance of Fifo.zip() operations
- **Key Metrics**: Zip operation time, throughput under various loads
- **Scenarios**: Different numbers of sources (2, 3, 5), high-throughput scenarios

### 3. TimeSeries Operations Benchmarks (`TimeSeriesBenchmark`)
- **Purpose**: Measure performance of TimeSeries creation and access operations
- **Key Metrics**: Creation time, value access time
- **Scenarios**: Small/large series creation, value/timestamp access patterns

### 4. Strategy Execution Benchmarks (`StrategyExecutionBenchmark`)
- **Purpose**: Measure performance of strategy execution and management
- **Key Metrics**: Strategy execution time, manager creation time
- **Scenarios**: Simple and complex strategy execution, multiple concurrent strategies

## Running Benchmarks

### Prerequisites
- Java 25 or higher
- Maven 3.8 or higher
- At least 2GB of free RAM

### Basic Commands

#### Run all benchmarks:
```bash
java -jar engine/benchmarks/target/benchmarks.jar
```

#### Run specific benchmark class:
```bash
java -jar engine/benchmarks/target/benchmarks.jar RuleEvaluationBenchmark
```

#### Run specific benchmark method:
```bash
java -jar engine/benchmarks/target/benchmarks.jar RuleEvaluationBenchmark.benchmarkCrossedRuleEvaluation
```

#### Run with custom parameters:
```bash
# Run with 3 iterations, 2 warmup iterations, 1 fork, 1 thread
java -jar engine/benchmarks/target/benchmarks.jar -i 3 -wi 2 -f 1 -t 1

# Run with verbose output
java -jar engine/benchmarks/target/benchmarks.jar -v EXTRA

# Run with specific time units
java -jar engine/benchmarks/target/benchmarks.jar -tu ms
```

### Advanced Options

#### Memory Configuration
The benchmarks are configured to use 2GB heap by default. You can override this:
```bash
java -Xms4G -Xmx4G -jar engine/benchmarks/target/benchmarks.jar
```

#### Profiling Integration
```bash
# With perf profiler
java -jar engine/benchmarks/target/benchmarks.jar -prof perfasm

# With Hotspot compiler profiling
java -jar engine/benchmarks/target/benchmarks.jar -prof hs_comp
```

## Building Benchmarks

### Clean build:
```bash
cd engine/benchmarks
mvn clean package
```

### Skip tests during build:
```bash
mvn clean package -DskipTests
```

## Best Practices

### 1. Environment Considerations
- **Run on production-like hardware** for meaningful results
- **Close other applications** during benchmark runs to reduce noise
- **Disable CPU frequency scaling** and turbo boost if possible
- **Use consistent JVM versions** when comparing results over time
- **Run benchmarks multiple times** to ensure consistency

### 2. Interpretation Guidelines
- **Focus on trends** rather than individual measurements
- **Compare against baselines** rather than absolute values
- **Consider confidence intervals** provided in results
- **Account for measurement overhead** in microsecond-range benchmarks
- **Validate that benchmarks test intended scenarios**

### 3. Performance Testing Workflow
1. **Establish baseline**: Run benchmarks on known good version
2. **Test changes**: Run benchmarks on new code
3. **Analyze differences**: Look for statistically significant changes
4. **Document findings**: Record performance impact of changes
5. **Repeat**: Verify results are reproducible

### 4. Writing New Benchmarks

When adding new benchmarks, follow these patterns:

```java
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class NewBenchmark {

    @Setup
    public void setup() {
        // Initialize benchmark state here
    }

    @Benchmark
    public ReturnType benchmarkMethod() {
        // The actual code to benchmark
        // Keep this method focused and minimal
    }
}
```

### 5. Common Pitfalls to Avoid
- **Don't trust first run**: Always allow for warmup
- **Avoid microbenchmarks for I/O**: Focus on CPU-intensive operations
- **Don't optimize prematurely**: Profile first, then benchmark optimizations
- **Be aware of dead code elimination**: Ensure benchmarks actually compute something
- **Watch for measurement bias**: Consider if benchmark setup affects results

### 6. Performance Regression Detection
- **Set up CI integration** to run benchmarks automatically
- **Define acceptable thresholds** for performance changes
- **Monitor multiple metrics** (throughput, latency, memory usage)
- **Track historical trends** to identify gradual degradation

## Maintenance Guidelines

### Regular Maintenance
- **Review benchmarks quarterly** to ensure they remain relevant
- **Update critical path coverage** as system evolves
- **Verify benchmark accuracy** after major system changes
- **Clean up obsolete benchmarks** that no longer reflect critical paths

### Documentation Updates
- **Keep this document synchronized** with actual benchmarks
- **Update expected performance ranges** when significant changes occur
- **Document any special requirements** for running specific benchmarks

## Troubleshooting

### Common Issues
- **OutOfMemoryError**: Increase heap size with `-Xmx` parameter
- **Long runtimes**: Reduce measurement iterations or fork count
- **Noisy results**: Run on isolated hardware with minimal background activity
- **Compilation errors**: Ensure JMH dependencies are correctly configured

### Verification Steps
1. Check that all benchmarks are discovered: `java -jar target/benchmarks.jar -l`
2. Run a quick test: `java -jar target/benchmarks.jar -i 1 -wi 0 -f 1 -t 1`
3. Verify reasonable performance ranges based on expected behavior

## Contact and Support

For questions about benchmark usage or to report issues:
- Add an issue in the GitHub repository
- Reference specific benchmark classes and parameters when reporting problems
- Include system specifications and JVM version when seeking help with results interpretation
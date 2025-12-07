# Ray性能优化实战技巧：释放分布式计算的最大潜力

## 概述

Ray作为高性能分布式计算框架，其性能优化涉及多个层面，从代码编写到集群配置都需要精心设计。本文基于Ray源码分析和实际项目经验，提供一套完整的性能优化指南和实战技巧。

## 任务并行化优化

### 1. 智能任务切分

```python
import ray
import numpy as np
from typing import List, Tuple
import time

# 优化前：粗粒度任务
@ray.remote
def process_large_dataset_large_chunk(data_chunk):
    """处理大数据块的函数 - 性能较差"""
    # 处理100MB数据块
    result = expensive_computation(data_chunk)
    return result

def process_dataset_optimized(data):
    """优化的数据处理函数"""
    chunk_size = len(data) // ray.available_resources().get('CPU', 1)

    # 优化后：动态任务切分
    @ray.remote
    def process_adaptive_chunk(data_slice):
        start_time = time.time()
        result = expensive_computation(data_slice)
        execution_time = time.time() - start_time

        # 根据执行时间调整后续任务大小
        return result, execution_time, len(data_slice)

    # 第一批任务用于测试性能
    test_chunks = np.array_split(data[:chunk_size * 4], 4)
    test_futures = [process_adaptive_chunk.remote(chunk) for chunk in test_chunks]

    # 收集性能数据
    test_results = ray.get(test_futures)

    # 计算最优任务大小
    avg_times = [result[1] for result in test_results]
    avg_chunk_sizes = [result[2] for result in test_results]

    # 目标执行时间为1-2秒
    target_time = 1.5
    avg_speed = sum(avg_chunk_sizes) / sum(avg_times)
    optimal_chunk_size = int(avg_speed * target_time)

    print(f"Optimal chunk size: {optimal_chunk_size} elements")

    # 使用最优大小切分剩余数据
    remaining_data = data[chunk_size * 4:]
    optimized_chunks = [remaining_data[i:i+optimal_chunk_size]
                       for i in range(0, len(remaining_data), optimal_chunk_size)]

    # 并行处理
    futures = [process_adaptive_chunk.remote(chunk) for chunk in optimized_chunks]
    results = ray.get(futures)

    return [result[0] for result in results]
```

### 2. 任务依赖优化

```python
class TaskDependencyOptimizer:
    """任务依赖优化器"""

    def __init__(self):
        self.task_graph = {}
        self.execution_plan = []

    def build_task_graph(self, tasks, dependencies):
        """构建任务依赖图"""
        for task_id, task in tasks.items():
            self.task_graph[task_id] = {
                'task': task,
                'dependencies': dependencies.get(task_id, []),
                'dependents': [],
                'priority': 0
            }

        # 构建反向依赖关系
        for task_id, task_info in self.task_graph.items():
            for dep_id in task_info['dependencies']:
                if dep_id in self.task_graph:
                    self.task_graph[dep_id]['dependents'].append(task_id)

    def optimize_execution_order(self):
        """优化执行顺序"""
        # 计算任务优先级（基于依赖深度）
        self._calculate_priorities()

        # 生成执行计划
        executed = set()
        execution_order = []

        while len(executed) < len(self.task_graph):
            # 找出所有依赖已满足的任务
            ready_tasks = [
                task_id for task_id, task_info in self.task_graph.items()
                if task_id not in executed and
                all(dep in executed for dep in task_info['dependencies'])
            ]

            # 按优先级排序
            ready_tasks.sort(key=lambda x: self.task_graph[x]['priority'], reverse=True)

            if not ready_tasks:
                raise RuntimeError("Circular dependency detected")

            # 添加到执行计划
            execution_order.extend(ready_tasks)
            executed.update(ready_tasks)

        return execution_order

    def _calculate_priorities(self):
        """计算任务优先级"""
        def calculate_depth(task_id, visited=None):
            if visited is None:
                visited = set()

            if task_id in visited:
                return 0  # 避免循环依赖

            visited.add(task_id)

            task_info = self.task_graph[task_id]
            if not task_info['dependents']:
                return 1

            max_depth = max(calculate_depth(dep_id, visited.copy())
                          for dep_id in task_info['dependents'])
            return max_depth + 1

        for task_id in self.task_graph:
            self.task_graph[task_id]['priority'] = calculate_depth(task_id)

# 使用示例
def optimized_pipeline_with_dependencies():
    """带依赖优化的管道"""

    @ray.remote
    def load_data(source):
        """数据加载"""
        time.sleep(0.1)  # 模拟IO操作
        return np.random.rand(1000, 100)

    @ray.remote
    def preprocess_data(data):
        """数据预处理"""
        time.sleep(0.2)
        return data / data.max()

    @ray.remote
    def extract_features(preprocessed_data):
        """特征提取"""
        time.sleep(0.3)
        return np.mean(preprocessed_data, axis=0)

    @ray.remote
    def train_model(features_list):
        """模型训练"""
        combined_features = np.concatenate(features_list)
        time.sleep(0.5)
        return {"accuracy": np.random.rand()}

    # 定义任务
    tasks = {
        'load1': load_data.remote("source1"),
        'load2': load_data.remote("source2"),
        'load3': load_data.remote("source3"),
        'preprocess1': None,
        'preprocess2': None,
        'preprocess3': None,
        'features1': None,
        'features2': None,
        'features3': None,
        'train': None
    }

    # 定义依赖关系
    dependencies = {
        'preprocess1': ['load1'],
        'preprocess2': ['load2'],
        'preprocess3': ['load3'],
        'features1': ['preprocess1'],
        'features2': ['preprocess2'],
        'features3': ['preprocess3'],
        'train': ['features1', 'features2', 'features3']
    }

    # 构建依赖图并优化
    optimizer = TaskDependencyOptimizer()
    optimizer.build_task_graph(tasks, dependencies)

    # 执行优化后的计划
    execution_order = optimizer.optimize_execution_order()

    # 按优化顺序提交任务
    for task_id in execution_order:
        if task_id.startswith('load'):
            continue  # 已创建
        elif task_id.startswith('preprocess'):
            source_id = f"load{task_id[-1]}"
            tasks[task_id] = preprocess_data.remote(tasks[source_id])
        elif task_id.startswith('features'):
            source_id = f"preprocess{task_id[-1]}"
            tasks[task_id] = extract_features.remote(tasks[source_id])
        elif task_id == 'train':
            feature_tasks = [tasks[f'features{i}'] for i in range(1, 4)]
            tasks[task_id] = train_model.remote(feature_tasks)

    return ray.get(tasks['train'])
```

## 内存优化策略

### 1. 对象引用管理优化

```python
class MemoryOptimizedObjectManager:
    """内存优化的对象管理器"""

    def __init__(self, max_cached_objects=100):
        self.max_cached_objects = max_cached_objects
        self.object_cache = {}
        self.object_access_times = {}
        self.object_sizes = {}

    @ray.remote
    def create_large_object_with_cleanup(self, data, object_id):
        """创建带清理的大型对象"""
        import psutil
        import gc

        # 检查内存使用
        memory_usage = psutil.virtual_memory().percent
        if memory_usage > 80:
            # 触发垃圾回收
            gc.collect()

            # 清理缓存的对象
            self._cleanup_cached_objects()

        # 存储对象
        obj_ref = ray.put(data)

        # 记录对象信息
        self.object_cache[object_id] = obj_ref
        self.object_access_times[object_id] = time.time()
        self.object_sizes[object_id] = len(str(data))  # 粗略估算

        return obj_ref

    def _cleanup_cached_objects(self):
        """清理缓存的对象"""
        if len(self.object_cache) <= self.max_cached_objects:
            return

        # 按访问时间排序，删除最久未访问的对象
        sorted_objects = sorted(
            self.object_access_times.items(),
            key=lambda x: x[1]
        )

        objects_to_remove = len(self.object_cache) - self.max_cached_objects

        for object_id, _ in sorted_objects[:objects_to_remove]:
            if object_id in self.object_cache:
                del self.object_cache[object_id]
                del self.object_access_times[object_id]
                del self.object_sizes[object_id]

        # 强制垃圾回收
        import gc
        gc.collect()

# 对象池优化
@ray.remote
class ObjectPool:
    """对象池管理Actor"""

    def __init__(self, pool_size=10):
        self.pool_size = pool_size
        self.available_objects = []
        self.used_objects = set()
        self.object_creators = {}

    def register_creator(self, object_type, creator_func):
        """注册对象创建函数"""
        self.object_creators[object_type] = creator_func

    def acquire(self, object_type):
        """从池中获取对象"""
        # 检查是否有可用对象
        for i, obj in enumerate(self.available_objects):
            if obj['type'] == object_type:
                obj_ref = obj['ref']
                self.available_objects.pop(i)
                self.used_objects.add(obj_ref)
                return obj_ref

        # 没有可用对象，创建新对象
        if len(self.used_objects) < self.pool_size:
            if object_type in self.object_creators:
                new_obj_ref = self.object_creators[object_type].remote()
                self.used_objects.add(new_obj_ref)
                return new_obj_ref

        raise RuntimeError(f"Pool exhausted for object type: {object_type}")

    def release(self, obj_ref, object_type):
        """释放对象回池中"""
        if obj_ref in self.used_objects:
            self.used_objects.remove(obj_ref)

            # 重置对象状态（如果需要）
            try:
                ray.get(obj_ref.reset.remote())
            except:
                pass  # 忽略重置错误

            self.available_objects.append({
                'ref': obj_ref,
                'type': object_type,
                'timestamp': time.time()
            })
```

### 2. 数据传输优化

```python
class DataTransferOptimizer:
    """数据传输优化器"""

    def __init__(self):
        self.compression_threshold = 1024 * 1024  # 1MB
        self.batch_size = 100

    def optimized_put(self, data):
        """优化的数据存储"""
        data_size = len(str(data)) if hasattr(data, '__len__') else 0

        # 大数据压缩存储
        if data_size > self.compression_threshold:
            return self._compressed_put(data)
        else:
            return ray.put(data)

    def _compressed_put(self, data):
        """压缩存储数据"""
        import zlib
        import pickle

        # 序列化数据
        serialized = pickle.dumps(data)

        # 压缩数据
        compressed = zlib.compress(serialized, level=3)

        # 存储压缩数据
        compressed_ref = ray.put(compressed)

        return {
            'compressed_data': compressed_ref,
            'original_size': len(serialized),
            'compressed_size': len(compressed),
            'compression_ratio': len(compressed) / len(serialized)
        }

    @staticmethod
    def optimized_get(obj_ref):
        """优化的数据获取"""
        if isinstance(obj_ref, dict) and 'compressed_data' in obj_ref:
            # 解压缩数据
            import zlib
            import pickle

            compressed_data = ray.get(obj_ref['compressed_data'])
            serialized = zlib.decompress(compressed_data)
            return pickle.loads(serialized)
        else:
            return ray.get(obj_ref)

# 批量数据传输优化
def batch_data_transfer(data_list, batch_size=10):
    """批量数据传输"""
    futures = []

    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i+batch_size]

        # 将批次数据打包
        batch_ref = ray.put(batch)

        # 创建批处理任务
        @ray.remote
        def process_batch(batch_data):
            """批处理任务"""
            results = []
            for item in batch_data:
                result = expensive_operation(item)
                results.append(result)
            return results

        future = process_batch.remote(batch_ref)
        futures.append(future)

    # 收集所有结果
    batch_results = ray.get(futures)

    # 展平结果
    final_results = []
    for batch_result in batch_results:
        final_results.extend(batch_result)

    return final_results
```

## 资源调度优化

### 1. 动态资源分配

```python
class DynamicResourceScheduler:
    """动态资源调度器"""

    def __init__(self):
        self.resource_pool = {}
        self.task_queue = []
        self.monitoring_thread = None
        self.is_monitoring = False

    def start_monitoring(self):
        """启动资源监控"""
        self.is_monitoring = True

        def monitor_resources():
            while self.is_monitoring:
                try:
                    # 获取当前资源状态
                    available_resources = ray.available_resources()

                    # 更新资源池状态
                    self._update_resource_pool(available_resources)

                    # 尝试调度等待中的任务
                    self._schedule_pending_tasks()

                    time.sleep(5)  # 每5秒检查一次

                except Exception as e:
                    print(f"Resource monitoring error: {e}")
                    time.sleep(5)

        self.monitoring_thread = threading.Thread(target=monitor_resources, daemon=True)
        self.monitoring_thread.start()

    def submit_task_with_dynamic_resources(self, task_func, min_resources, max_resources=None):
        """提交带动态资源的任务"""
        task_spec = {
            'func': task_func,
            'min_resources': min_resources,
            'max_resources': max_resources or min_resources,
            'current_resources': min_resources,
            'submission_time': time.time(),
            'status': 'pending'
        }

        self.task_queue.append(task_spec)
        return task_spec

    def _update_resource_pool(self, available_resources):
        """更新资源池"""
        self.resource_pool = available_resources.copy()

    def _schedule_pending_tasks(self):
        """调度待处理任务"""
        for task_spec in self.task_queue:
            if task_spec['status'] == 'pending':
                if self._can_schedule_task(task_spec):
                    self._execute_task(task_spec)

    def _can_schedule_task(self, task_spec):
        """检查是否可以调度任务"""
        required_resources = task_spec['current_resources']

        for resource_name, required_amount in required_resources.items():
            available_amount = self.resource_pool.get(resource_name, 0)
            if available_amount < required_amount:
                return False

        return True

    def _execute_task(self, task_spec):
        """执行任务"""
        task_func = task_spec['func']
        resources = task_spec['current_resources']

        # 创建带资源约束的任务
        @ray.remote(**{f'{k}_resources': v for k, v in resources.items()})
        def dynamic_task(*args, **kwargs):
            return task_func(*args, **kwargs)

        # 更新任务状态
        task_spec['status'] = 'running'
        task_spec['start_time'] = time.time()

# 自适应调度策略
class AdaptiveSchedulingStrategy:
    """自适应调度策略"""

    def __init__(self):
        self.performance_history = {}
        self.resource_usage_history = []

    def select_scheduling_strategy(self, task_info, available_nodes):
        """选择最优调度策略"""
        task_complexity = self._estimate_task_complexity(task_info)
        cluster_load = self._calculate_cluster_load()

        if task_complexity == 'high' and cluster_load < 0.7:
            # 高复杂度任务，集群负载低，使用节点亲和性
            return self._create_node_affinity_strategy(available_nodes)
        elif task_complexity == 'low':
            # 低复杂度任务，使用分散调度
            return self._create_spread_strategy()
        else:
            # 默认策略
            return self._create_default_strategy()

    def _estimate_task_complexity(self, task_info):
        """估算任务复杂度"""
        # 基于任务类型、资源需求等估算复杂度
        resource_sum = sum(task_info.get('resources', {}).values())

        if resource_sum > 4:
            return 'high'
        elif resource_sum < 1:
            return 'low'
        else:
            return 'medium'
```

## 缓存和预计算优化

### 1. 智能缓存系统

```python
@ray.remote
class IntelligentCache:
    """智能缓存Actor"""

    def __init__(self, max_size=1000, ttl=3600):
        self.max_size = max_size
        self.ttl = ttl
        self.cache = {}
        self.access_times = {}
        self.hit_count = 0
        self.miss_count = 0

    def get(self, key):
        """获取缓存值"""
        if key in self.cache:
            item = self.cache[key]

            # 检查TTL
            if time.time() - item['timestamp'] < self.ttl:
                # 更新访问时间
                self.access_times[key] = time.time()
                self.hit_count += 1

                return item['value']
            else:
                # 过期，删除
                del self.cache[key]
                if key in self.access_times:
                    del self.access_times[key]

        self.miss_count += 1
        return None

    def put(self, key, value):
        """存储缓存值"""
        # 检查容量
        if len(self.cache) >= self.max_size:
            self._evict_lru_items()

        self.cache[key] = {
            'value': value,
            'timestamp': time.time()
        }
        self.access_times[key] = time.time()

    def get_stats(self):
        """获取缓存统计"""
        total_requests = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_requests if total_requests > 0 else 0

        return {
            'hit_count': self.hit_count,
            'miss_count': self.miss_count,
            'hit_rate': hit_rate,
            'cache_size': len(self.cache)
        }

    def _evict_lru_items(self, evict_count=100):
        """删除最近最少使用的项目"""
        # 按访问时间排序
        sorted_items = sorted(
            self.access_times.items(),
            key=lambda x: x[1]
        )

        # 删除最久未访问的项目
        for i, (key, _) in enumerate(sorted_items):
            if i >= evict_count:
                break

            if key in self.cache:
                del self.cache[key]
            del self.access_times[key]

# 预计算优化
class PrecomputationOptimizer:
    """预计算优化器"""

    def __init__(self):
        self.precomputed_tasks = {}
        self.dependency_graph = {}

    def register_precomputable_task(self, task_id, task_func, dependencies=None):
        """注册可预计算任务"""
        self.precomputed_tasks[task_id] = {
            'func': task_func,
            'dependencies': dependencies or [],
            'result': None,
            'computed_at': None,
            'is_computed': False
        }

        # 构建依赖图
        self.dependency_graph[task_id] = dependencies or []

    def precompute_batch(self, task_ids):
        """批量预计算"""
        # 按依赖关系排序
        sorted_tasks = self._topological_sort(task_ids)

        results = {}

        for task_id in sorted_tasks:
            if task_id not in self.precomputed_tasks:
                continue

            task_info = self.precomputed_tasks[task_id]

            if not task_info['is_computed']:
                # 获取依赖结果
                dependency_results = []
                for dep_id in task_info['dependencies']:
                    if dep_id in results:
                        dependency_results.append(results[dep_id])

                # 执行预计算
                @ray.remote
                def execute_precomputation(func, deps):
                    return func(*deps)

                result_ref = execute_precomputation.remote(
                    task_info['func'],
                    dependency_results
                )

                result = ray.get(result_ref)

                # 保存结果
                task_info['result'] = result
                task_info['computed_at'] = time.time()
                task_info['is_computed'] = True

                results[task_id] = result
            else:
                results[task_id] = task_info['result']

        return results

    def _topological_sort(self, task_ids):
        """拓扑排序"""
        visited = set()
        result = []

        def visit(task_id):
            if task_id in visited or task_id not in self.precomputed_tasks:
                return

            visited.add(task_id)

            # 访问依赖
            for dep_id in self.dependency_graph.get(task_id, []):
                visit(dep_id)

            result.append(task_id)

        for task_id in task_ids:
            visit(task_id)

        return result
```

## 性能监控和调优

### 1. 性能分析器

```python
class RayPerformanceProfiler:
    """Ray性能分析器"""

    def __init__(self):
        self.task_metrics = {}
        self.node_metrics = {}
        self.is_profiling = False

    def start_profiling(self):
        """开始性能分析"""
        self.is_profiling = True

        def profiling_loop():
            while self.is_profiling:
                self._collect_metrics()
                time.sleep(1)  # 每秒收集一次指标

        threading.Thread(target=profiling_loop, daemon=True).start()

    def stop_profiling(self):
        """停止性能分析"""
        self.is_profiling = False

    def _collect_metrics(self):
        """收集性能指标"""
        # 收集节点指标
        nodes = ray.nodes()
        current_time = time.time()

        for node in nodes:
            node_id = node['NodeID']

            if node_id not in self.node_metrics:
                self.node_metrics[node_id] = []

            node_metric = {
                'timestamp': current_time,
                'cpu_usage': node['Resources'].get('CPU', 0),
                'memory_usage': node['Resources'].get('memory', 0),
                'gpu_usage': node['Resources'].get('GPU', 0),
                'object_store_memory': node['ObjectStoreMemory'] / (1024**3),  # GB
                'alive': node['Alive']
            }

            self.node_metrics[node_id].append(node_metric)

        # 保留最近5分钟的数据
        cutoff_time = current_time - 300
        for node_id in self.node_metrics:
            self.node_metrics[node_id] = [
                metric for metric in self.node_metrics[node_id]
                if metric['timestamp'] > cutoff_time
            ]

    def generate_performance_report(self):
        """生成性能报告"""
        report = {
            'timestamp': time.time(),
            'node_summary': {},
            'bottlenecks': [],
            'recommendations': []
        }

        # 分析节点性能
        for node_id, metrics in self.node_metrics.items():
            if not metrics:
                continue

            cpu_usage = [m['cpu_usage'] for m in metrics if m['alive']]
            memory_usage = [m['object_store_memory'] for m in metrics if m['alive']]

            report['node_summary'][node_id] = {
                'avg_cpu_usage': sum(cpu_usage) / len(cpu_usage) if cpu_usage else 0,
                'max_cpu_usage': max(cpu_usage) if cpu_usage else 0,
                'avg_memory_gb': sum(memory_usage) / len(memory_usage) if memory_usage else 0,
                'max_memory_gb': max(memory_usage) if memory_usage else 0,
                'alive_time': sum(1 for m in metrics if m['alive'])
            }

            # 检测性能瓶颈
            if memory_usage and max(memory_usage) > 50:  # 超过50GB
                report['bottlenecks'].append({
                    'type': 'high_memory_usage',
                    'node_id': node_id,
                    'value': max(memory_usage),
                    'threshold': 50
                })

            if cpu_usage and max(cpu_usage) > 0.9:  # CPU使用率超过90%
                report['bottlenecks'].append({
                    'type': 'high_cpu_usage',
                    'node_id': node_id,
                    'value': max(cpu_usage),
                    'threshold': 0.9
                })

        # 生成优化建议
        report['recommendations'] = self._generate_recommendations(report)

        return report

    def _generate_recommendations(self, report):
        """生成优化建议"""
        recommendations = []

        for bottleneck in report['bottlenecks']:
            if bottleneck['type'] == 'high_memory_usage':
                recommendations.append({
                    'type': 'memory_optimization',
                    'priority': 'high',
                    'description': f"Node {bottleneck['node_id']} has high memory usage ({bottleneck['value']:.1f}GB)",
                    'suggestions': [
                        "Enable object spilling to disk",
                        "Increase object store memory",
                        "Implement object reference cleanup",
                        "Use compression for large objects"
                    ]
                })
            elif bottleneck['type'] == 'high_cpu_usage':
                recommendations.append({
                    'type': 'cpu_optimization',
                    'priority': 'medium',
                    'description': f"Node {bottleneck['node_id']} has high CPU usage ({bottleneck['value']:.1%})",
                    'suggestions': [
                        "Add more CPU resources",
                        "Optimize task granularity",
                        "Use vectorized operations",
                        "Consider GPU acceleration"
                    ]
                })

        return recommendations

# 性能测试工具
class RayBenchmark:
    """Ray基准测试工具"""

    def __init__(self):
        self.test_results = {}

    def benchmark_task_throughput(self, task_func, num_tasks=1000):
        """测试任务吞吐量"""
        @ray.remote
        def benchmark_task(*args, **kwargs):
            return task_func(*args, **kwargs)

        # 预热
        warmup_futures = [benchmark_task.remote() for _ in range(10)]
        ray.get(warmup_futures)

        # 正式测试
        start_time = time.time()

        futures = [benchmark_task.remote() for _ in range(num_tasks)]
        results = ray.get(futures)

        end_time = time.time()

        duration = end_time - start_time
        throughput = num_tasks / duration

        return {
            'num_tasks': num_tasks,
            'duration': duration,
            'throughput': throughput,
            'avg_task_time': duration / num_tasks
        }

    def benchmark_data_transfer(self, data_sizes):
        """测试数据传输性能"""
        results = {}

        for size in data_sizes:
            # 生成测试数据
            test_data = np.random.rand(size)

            # 测试存储性能
            start_time = time.time()
            obj_ref = ray.put(test_data)
            put_time = time.time() - start_time

            # 测试获取性能
            start_time = time.time()
            retrieved_data = ray.get(obj_ref)
            get_time = time.time() - start_time

            results[f"{size}_elements"] = {
                'put_time': put_time,
                'get_time': get_time,
                'total_time': put_time + get_time,
                'throughput_mbps': (size * 8) / (put_time + get_time) / (1024**2)  # Mbps
            }

        return results
```

## 总结

Ray性能优化需要从多个维度综合考虑：

1. **任务并行化**：智能切分、依赖优化、执行顺序优化
2. **内存管理**：对象引用管理、数据传输优化、缓存策略
3. **资源调度**：动态资源分配、自适应调度策略
4. **缓存优化**：智能缓存、预计算、结果复用
5. **性能监控**：实时监控、瓶颈检测、自动调优
6. **基准测试**：性能测试、吞吐量分析、优化验证

通过这些优化技巧，可以显著提升Ray分布式应用的性能，充分发挥分布式计算的潜力。

## 参考源码路径

- 任务调度优化：`/ray/python/ray/_private/scheduling_strategy.py`
- 内存管理：`/ray/python/ray/_private/memory_manager.py`
- 性能监控：`/ray/python/ray/dashboard/modules/metrics.py`
- 对象存储：`/ray/python/ray/_private/plasma_client.py`
- 缓存系统：`/ray/python/ray/_private/util.py`
- 性能测试：`/ray/python/ray/tests/test_performance.py`
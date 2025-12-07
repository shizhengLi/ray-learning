# Ray错误处理与容错机制：构建高可靠分布式系统的基石

## 概述

Ray的错误处理和容错机制是其分布式计算框架的核心优势之一，通过多层次的设计确保系统在节点故障、网络分区、任务失败等各种异常情况下的稳定运行。本文将深入分析Ray容错系统的实现原理和最佳实践。

## 容错架构设计

### 1. 分层容错策略

Ray采用多层次的容错策略：

```
应用层容错 (用户重试逻辑)
    ↓
任务层容错 (Task重试机制)
    ↓
Actor容错 (Actor重启策略)
    ↓
对象容错 (对象重建机制)
    ↓
节点容错 (故障检测和恢复)
    ↓
集群容错 (全局控制服务冗余)
```

### 2. 容错核心组件

```python
# /ray/python/ray/_private/fault_tolerance.py
class FaultToleranceManager:
    """容错管理器"""

    def __init__(self, gcs_client, raylet_client):
        self.gcs_client = gcs_client
        self.raylet_client = raylet_client

        # 容错组件
        self.failure_detector = FailureDetector()
        self.recovery_manager = RecoveryManager()
        self.retry_policy = RetryPolicy()
        self.checkpoint_manager = CheckpointManager()

        # 容错配置
        self.config = self._load_fault_tolerance_config()

        # 启动容错监控
        self._start_fault_tolerance_monitoring()

    def _load_fault_tolerance_config(self):
        """加载容错配置"""
        return {
            'max_task_retries': 3,
            'max_actor_restarts': 3,
            'retry_delay_base': 1.0,
            'retry_delay_multiplier': 2.0,
            'failure_detection_timeout': 30,
            'recovery_timeout': 300,
            'checkpoint_interval': 60
        }

    def _start_fault_tolerance_monitoring(self):
        """启动容错监控"""
        def monitoring_loop():
            while True:
                try:
                    # 检测故障
                    failures = self.failure_detector.detect_failures()

                    # 处理检测到的故障
                    for failure in failures:
                        self._handle_failure(failure)

                    # 检查超时的恢复任务
                    self._check_recovery_timeouts()

                    time.sleep(5)  # 每5秒检查一次

                except Exception as e:
                    logger.error(f"Error in fault tolerance monitoring: {e}")
                    time.sleep(5)

        monitor_thread = threading.Thread(target=monitoring_loop, daemon=True)
        monitor_thread.start()

    def _handle_failure(self, failure):
        """处理故障"""
        failure_type = failure['type']
        failure_id = failure['id']

        try:
            logger.info(f"Handling failure: {failure_type} - {failure_id}")

            if failure_type == 'node_failure':
                self.recovery_manager.handle_node_failure(failure)
            elif failure_type == 'task_failure':
                self.recovery_manager.handle_task_failure(failure)
            elif failure_type == 'actor_failure':
                self.recovery_manager.handle_actor_failure(failure)
            elif failure_type == 'object_loss':
                self.recovery_manager.handle_object_loss(failure)
            else:
                logger.warning(f"Unknown failure type: {failure_type}")

        except Exception as e:
            logger.error(f"Error handling failure {failure_id}: {e}")
```

## 故障检测机制

### 1. 节点故障检测

```python
class FailureDetector:
    """故障检测器"""

    def __init__(self):
        self.node_status = {}  # 节点状态缓存
        self.last_heartbeat = {}  # 最后心跳时间
        self.failure_threshold = 30  # 故障检测阈值（秒）
        self.heartbeat_interval = 5  # 心跳检查间隔

        # 启动心跳监控
        self._start_heartbeat_monitoring()

    def _start_heartbeat_monitoring(self):
        """启动心跳监控"""
        def heartbeat_loop():
            while True:
                try:
                    current_time = time.time()

                    # 检查所有节点的 heartbeat
                    for node_id, last_heartbeat_time in list(self.last_heartbeat.items()):
                        if current_time - last_heartbeat_time > self.failure_threshold:
                            # 节点可能故障
                            self._handle_potential_node_failure(node_id)

                    time.sleep(self.heartbeat_interval)

                except Exception as e:
                    logger.error(f"Error in heartbeat monitoring: {e}")
                    time.sleep(self.heartbeat_interval)

        heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        heartbeat_thread.start()

    def update_node_heartbeat(self, node_id, node_info):
        """更新节点心跳"""
        current_time = time.time()
        self.last_heartbeat[node_id] = current_time
        self.node_status[node_id] = {
            'status': 'alive',
            'last_heartbeat': current_time,
            'info': node_info
        }

    def _handle_potential_node_failure(self, node_id):
        """处理潜在节点故障"""
        if node_id in self.node_status and self.node_status[node_id]['status'] == 'alive':
            # 标记为可疑故障
            self.node_status[node_id]['status'] = 'suspected_dead'

            logger.warning(f"Node {node_id} suspected dead, attempting verification")

            # 启动故障验证
            self._verify_node_failure(node_id)

    def _verify_node_failure(self, node_id):
        """验证节点故障"""
        try:
            # 尝试连接到节点
            node_info = self.node_status[node_id]['info']
            node_address = node_info.get('node_manager_address')
            node_port = node_info.get('node_manager_port')

            if node_address and node_port:
                # 发送健康检查请求
                is_healthy = self._send_health_check(node_address, node_port)

                if is_healthy:
                    # 节点仍然健康，更新心跳
                    self.update_node_heartbeat(node_id, node_info)
                    logger.info(f"Node {node_id} health check passed")
                else:
                    # 确认节点故障
                    self._confirm_node_failure(node_id)

            else:
                # 无法进行健康检查，直接确认故障
                self._confirm_node_failure(node_id)

        except Exception as e:
            logger.error(f"Error verifying node failure for {node_id}: {e}")
            self._confirm_node_failure(node_id)

    def _send_health_check(self, address, port):
        """发送健康检查请求"""
        try:
            import socket

            # 创建socket连接
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 5秒超时

            result = sock.connect_ex((address, port))
            sock.close()

            return result == 0  # 连接成功返回0

        except Exception:
            return False

    def _confirm_node_failure(self, node_id):
        """确认节点故障"""
        if self.node_status[node_id]['status'] != 'dead':
            self.node_status[node_id]['status'] = 'dead'
            self.node_status[node_id]['failure_time'] = time.time()

            # 发布故障事件
            self._publish_failure_event({
                'type': 'node_failure',
                'id': node_id,
                'timestamp': time.time(),
                'node_info': self.node_status[node_id]['info']
            })

            logger.error(f"Node {node_id} confirmed dead")

    def detect_failures(self):
        """检测故障"""
        failures = []

        # 检测节点故障
        for node_id, status in self.node_status.items():
            if status['status'] == 'dead':
                failures.append({
                    'type': 'node_failure',
                    'id': node_id,
                    'timestamp': status.get('failure_time', time.time()),
                    'info': status.get('info', {})
                })

        return failures
```

### 2. 任务和Actor故障检测

```python
class TaskFailureDetector:
    """任务故障检测器"""

    def __init__(self, gcs_client):
        self.gcs_client = gcs_client
        self.task_timeouts = {}  # 任务超时配置
        self.running_tasks = {}  # 运行中任务状态

    def register_task(self, task_id, timeout=None):
        """注册任务监控"""
        task_timeout = timeout or self.task_timeouts.get('default', 300)  # 默认5分钟

        self.running_tasks[task_id] = {
            'start_time': time.time(),
            'timeout': task_timeout,
            'status': 'running'
        }

    def mark_task_completed(self, task_id, success=True):
        """标记任务完成"""
        if task_id in self.running_tasks:
            self.running_tasks[task_id]['status'] = 'completed' if success else 'failed'
            self.running_tasks[task_id]['end_time'] = time.time()

    def detect_task_timeouts(self):
        """检测任务超时"""
        current_time = time.time()
        timeout_tasks = []

        for task_id, task_info in list(self.running_tasks.items()):
            if task_info['status'] == 'running':
                elapsed_time = current_time - task_info['start_time']
                if elapsed_time > task_info['timeout']:
                    timeout_tasks.append({
                        'type': 'task_timeout',
                        'id': task_id,
                        'elapsed_time': elapsed_time,
                        'timeout': task_info['timeout']
                    })

        return timeout_tasks

class ActorFailureDetector:
    """Actor故障检测器"""

    def __init__(self, gcs_client):
        self.gcs_client = gcs_client
        self.actor_status = {}  # Actor状态
        self.heartbeat_intervals = {}  # Actor心跳间隔

    def register_actor(self, actor_id, heartbeat_interval=10):
        """注册Actor监控"""
        self.actor_status[actor_id] = {
            'status': 'alive',
            'last_heartbeat': time.time(),
            'restart_count': 0
        }
        self.heartbeat_intervals[actor_id] = heartbeat_interval

    def update_actor_heartbeat(self, actor_id):
        """更新Actor心跳"""
        if actor_id in self.actor_status:
            self.actor_status[actor_id]['last_heartbeat'] = time.time()
            self.actor_status[actor_id]['status'] = 'alive'

    def detect_actor_failures(self):
        """检测Actor故障"""
        current_time = time.time()
        failed_actors = []

        for actor_id, status in list(self.actor_status.items()):
            if status['status'] == 'alive':
                heartbeat_interval = self.heartbeat_intervals.get(actor_id, 10)
                time_since_heartbeat = current_time - status['last_heartbeat']

                # 如果超过3个心跳间隔没有收到心跳，认为Actor故障
                if time_since_heartbeat > heartbeat_interval * 3:
                    failed_actors.append({
                        'type': 'actor_failure',
                        'id': actor_id,
                        'last_heartbeat': status['last_heartbeat'],
                        'time_since_heartbeat': time_since_heartbeat
                    })

                    # 标记Actor为故障状态
                    self.actor_status[actor_id]['status'] = 'failed'

        return failed_actors
```

## 恢复管理机制

### 1. 任务恢复

```python
class TaskRecoveryManager:
    """任务恢复管理器"""

    def __init__(self, gcs_client, retry_policy):
        self.gcs_client = gcs_client
        self.retry_policy = retry_policy
        self.retry_attempts = {}  # 重试次数记录
        self.task_lineage = {}    # 任务血缘信息

    def handle_task_failure(self, failure):
        """处理任务失败"""
        task_id = failure['id']

        try:
            # 获取任务信息
            task_info = self._get_task_info(task_id)
            if not task_info:
                logger.error(f"Task {task_id} not found for recovery")
                return False

            # 检查是否应该重试
            if self._should_retry_task(task_id, task_info):
                return self._retry_task(task_id, task_info)
            else:
                # 标记任务为最终失败
                self._mark_task_failed(task_id, failure)
                return False

        except Exception as e:
            logger.error(f"Error handling task failure for {task_id}: {e}")
            return False

    def _should_retry_task(self, task_id, task_info):
        """判断是否应该重试任务"""
        retry_count = self.retry_attempts.get(task_id, 0)
        max_retries = task_info.get('max_retries', self.retry_policy.max_task_retries)

        # 检查重试次数限制
        if retry_count >= max_retries:
            logger.warning(f"Task {task_id} exceeded max retries ({max_retries})")
            return False

        # 检查异常类型是否可重试
        failure_type = task_info.get('failure_type', 'unknown')
        if not self.retry_policy.is_retryable_failure(failure_type):
            logger.info(f"Task {task_id} failure type {failure_type} is not retryable")
            return False

        return True

    def _retry_task(self, task_id, task_info):
        """重试任务"""
        try:
            # 计算重试延迟
            retry_count = self.retry_attempts.get(task_id, 0)
            delay = self.retry_policy.calculate_retry_delay(retry_count)

            if delay > 0:
                logger.info(f"Retrying task {task_id} after {delay}s delay (attempt {retry_count + 1})")
                time.sleep(delay)

            # 重新提交任务
            new_task_id = self._resubmit_task(task_info)

            # 更新重试记录
            self.retry_attempts[task_id] = retry_count + 1

            # 记录重试血缘
            self._update_task_lineage(task_id, new_task_id)

            logger.info(f"Successfully retried task {task_id} as {new_task_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to retry task {task_id}: {e}")
            return False

    def _resubmit_task(self, task_info):
        """重新提交任务"""
        # 构建重新提交的任务规格
        task_spec = {
            'function_descriptor': task_info['function_descriptor'],
            'args': task_info['args'],
            'kwargs': task_info['kwargs'],
            'resources': task_info['resources'],
            'max_retries': task_info['max_retries'],
            'retry_exceptions': task_info['retry_exceptions'],
            'scheduling_strategy': task_info['scheduling_strategy']
        }

        # 提交任务到调度器
        new_task_id = ray._private.worker.global_worker.core_worker.submit_task(
            ray.Language.PYTHON,
            task_spec['function_descriptor'],
            task_spec['args'],
            task_spec['kwargs'],
            name=task_info.get('name', ''),
            num_returns=task_info.get('num_returns', 1),
            resources=task_spec['resources'],
            max_retries=task_spec['max_retries'],
            retry_exceptions=task_spec['retry_exceptions'],
            scheduling_strategy=task_spec['scheduling_strategy']
        )

        return new_task_id

    def _update_task_lineage(self, original_task_id, new_task_id):
        """更新任务血缘关系"""
        if original_task_id not in self.task_lineage:
            self.task_lineage[original_task_id] = {
                'original_id': original_task_id,
                'retry_chain': []
            }

        self.task_lineage[original_task_id]['retry_chain'].append({
            'task_id': new_task_id,
            'retry_time': time.time(),
            'retry_number': len(self.task_lineage[original_task_id]['retry_chain']) + 1
        })
```

### 2. Actor恢复

```python
class ActorRecoveryManager:
    """Actor恢复管理器"""

    def __init__(self, gcs_client):
        self.gcs_client = gcs_client
        self.actor_state = {}  # Actor状态缓存
        self.recovery_policies = {}  # 恢复策略

    def handle_actor_failure(self, failure):
        """处理Actor故障"""
        actor_id = failure['id']

        try:
            # 获取Actor信息
            actor_info = self._get_actor_info(actor_id)
            if not actor_info:
                logger.error(f"Actor {actor_id} not found for recovery")
                return False

            # 获取恢复策略
            recovery_policy = self._get_recovery_policy(actor_info)

            # 根据策略执行恢复
            if recovery_policy == 'restart':
                return self._restart_actor(actor_id, actor_info)
            elif recovery_policy == 'reconstruct':
                return self._reconstruct_actor(actor_id, actor_info)
            else:
                # 不进行恢复
                self._mark_actor_failed(actor_id, failure)
                return False

        except Exception as e:
            logger.error(f"Error handling actor failure for {actor_id}: {e}")
            return False

    def _restart_actor(self, actor_id, actor_info):
        """重启Actor"""
        try:
            logger.info(f"Attempting to restart actor {actor_id}")

            # 1. 停止故障的Actor实例
            self._stop_actor_instance(actor_id)

            # 2. 重新创建Actor实例
            new_actor_handle = self._create_actor_instance(actor_info)

            # 3. 恢复Actor状态（如果有检查点）
            if actor_info.get('checkpoint_enabled', False):
                self._restore_actor_state(actor_id, new_actor_handle)

            # 4. 更新Actor状态
            self._update_actor_state(actor_id, 'restarted', new_actor_handle)

            logger.info(f"Successfully restarted actor {actor_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to restart actor {actor_id}: {e}")
            return False

    def _reconstruct_actor(self, actor_id, actor_info):
        """重建Actor"""
        try:
            logger.info(f"Attempting to reconstruct actor {actor_id}")

            # 1. 获取Actor类信息
            actor_class_info = self._get_actor_class_info(actor_info)

            # 2. 重新创建Actor
            reconstructed_actor = ray.remote(
                **actor_class_info['remote_options']
            )(actor_class_info['actor_class'])

            # 3. 重新初始化Actor
            actor_handle = reconstructed_actor.remote()

            # 4. 重新建立Actor引用
            self._register_actor_handle(actor_id, actor_handle)

            # 5. 恢复Actor状态
            self._restore_actor_state(actor_id, actor_handle)

            logger.info(f"Successfully reconstructed actor {actor_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to reconstruct actor {actor_id}: {e}")
            return False

    def _restore_actor_state(self, actor_id, actor_handle):
        """恢复Actor状态"""
        try:
            # 获取最新的检查点
            checkpoint = self._get_latest_checkpoint(actor_id)

            if checkpoint:
                # 调用Actor的状态恢复方法
                restore_method = getattr(actor_handle, 'restore_state', None)
                if restore_method:
                    restore_method.remote(checkpoint['state_data'])
                    logger.info(f"Restored state for actor {actor_id} from checkpoint")
                else:
                    logger.warning(f"Actor {actor_id} does not implement restore_state method")

        except Exception as e:
            logger.error(f"Failed to restore state for actor {actor_id}: {e}")
```

## 检查点机制

### 1. 检查点管理器

```python
class CheckpointManager:
    """检查点管理器"""

    def __init__(self, storage_backend='local'):
        self.storage_backend = self._initialize_storage_backend(storage_backend)
        self.checkpoint_metadata = {}  # 检查点元数据
        self.checkpoint_retention = 10  # 保留的检查点数量

    def _initialize_storage_backend(self, backend_type):
        """初始化存储后端"""
        if backend_type == 'local':
            return LocalCheckpointBackend()
        elif backend_type == 's3':
            return S3CheckpointBackend()
        elif backend_type == 'gcs':
            return GCSCheckpointBackend()
        else:
            raise ValueError(f"Unsupported checkpoint backend: {backend_type}")

    def create_checkpoint(self, entity_id, entity_type, data, metadata=None):
        """创建检查点"""
        try:
            checkpoint_id = self._generate_checkpoint_id(entity_id)

            # 序列化数据
            serialized_data = self._serialize_data(data)

            # 存储检查点
            storage_location = self.storage_backend.store_checkpoint(
                checkpoint_id, serialized_data
            )

            # 创建检查点元数据
            checkpoint_info = {
                'checkpoint_id': checkpoint_id,
                'entity_id': entity_id,
                'entity_type': entity_type,
                'storage_location': storage_location,
                'creation_time': time.time(),
                'data_size': len(serialized_data),
                'metadata': metadata or {}
            }

            # 保存元数据
            self.checkpoint_metadata[checkpoint_id] = checkpoint_info

            # 清理旧检查点
            self._cleanup_old_checkpoints(entity_id)

            logger.info(f"Created checkpoint {checkpoint_id} for {entity_type} {entity_id}")
            return checkpoint_id

        except Exception as e:
            logger.error(f"Failed to create checkpoint for {entity_id}: {e}")
            return None

    def restore_checkpoint(self, entity_id, checkpoint_id=None):
        """恢复检查点"""
        try:
            if checkpoint_id is None:
                # 获取最新的检查点
                checkpoint_id = self._get_latest_checkpoint_id(entity_id)

            if checkpoint_id is None:
                logger.warning(f"No checkpoint found for entity {entity_id}")
                return None

            # 获取检查点元数据
            checkpoint_info = self.checkpoint_metadata.get(checkpoint_id)
            if not checkpoint_info:
                logger.error(f"Checkpoint metadata not found for {checkpoint_id}")
                return None

            # 从存储加载数据
            serialized_data = self.storage_backend.load_checkpoint(
                checkpoint_info['storage_location']
            )

            # 反序列化数据
            data = self._deserialize_data(serialized_data)

            logger.info(f"Restored checkpoint {checkpoint_id} for entity {entity_id}")
            return data

        except Exception as e:
            logger.error(f"Failed to restore checkpoint for {entity_id}: {e}")
            return None

    def _cleanup_old_checkpoints(self, entity_id):
        """清理旧检查点"""
        entity_checkpoints = [
            (checkpoint_id, info)
            for checkpoint_id, info in self.checkpoint_metadata.items()
            if info['entity_id'] == entity_id
        ]

        # 按创建时间排序，保留最新的N个
        entity_checkpoints.sort(key=lambda x: x[1]['creation_time'], reverse=True)

        # 删除多余的检查点
        for checkpoint_id, checkpoint_info in entity_checkpoints[self.checkpoint_retention:]:
            try:
                # 从存储删除
                self.storage_backend.delete_checkpoint(checkpoint_info['storage_location'])

                # 删除元数据
                del self.checkpoint_metadata[checkpoint_id]

                logger.debug(f"Deleted old checkpoint {checkpoint_id}")

            except Exception as e:
                logger.error(f"Failed to delete checkpoint {checkpoint_id}: {e}")

# 检查点装饰器
def checkpoint_actor(checkpoint_interval=60, storage_backend='local'):
    """Actor检查点装饰器"""
    def decorator(actor_class):
        original_init = actor_class.__init__
        original_methods = {}

        # 拦截__init__方法
        def __init__(self, *args, **kwargs):
            # 调用原始初始化
            original_init(self, *args, **kwargs)

            # 初始化检查点
            self._checkpoint_manager = CheckpointManager(storage_backend)
            self._checkpoint_enabled = True
            self._checkpoint_interval = checkpoint_interval
            self._last_checkpoint_time = time.time()

            # 启动检查点线程
            self._start_checkpoint_thread()

        # 拦截所有方法以支持检查点
        for attr_name in dir(actor_class):
            attr = getattr(actor_class, attr_name)
            if callable(attr) and not attr_name.startswith('_'):
                original_methods[attr_name] = attr

                def make_checkpoint_wrapper(method_name, original_method):
                    def wrapper(self, *args, **kwargs):
                        try:
                            # 执行原始方法
                            result = original_method(self, *args, **kwargs)

                            # 检查是否需要创建检查点
                            if self._checkpoint_enabled:
                                self._check_and_create_checkpoint()

                            return result

                        except Exception as e:
                            # 在异常情况下也创建检查点
                            if self._checkpoint_enabled:
                                self._create_checkpoint_now()

                            raise e

                    return wrapper

                setattr(actor_class, attr_name, make_checkpoint_wrapper(attr_name, attr))

        # 添加检查点方法
        def save_checkpoint(self):
            """手动保存检查点"""
            return self._create_checkpoint_now()

        def restore_checkpoint(self, checkpoint_id=None):
            """恢复检查点"""
            state_data = self._checkpoint_manager.restore_checkpoint(
                str(ray.get_runtime_context().get_actor_id()),
                checkpoint_id
            )

            if state_data:
                self._restore_state_from_checkpoint(state_data)

        def _start_checkpoint_thread(self):
            """启动检查点线程"""
            def checkpoint_loop():
                while True:
                    try:
                        time.sleep(self._checkpoint_interval)
                        self._check_and_create_checkpoint()
                    except Exception as e:
                        logger.error(f"Error in checkpoint thread: {e}")

            checkpoint_thread = threading.Thread(target=checkpoint_loop, daemon=True)
            checkpoint_thread.start()

        def _check_and_create_checkpoint(self):
            """检查并创建检查点"""
            current_time = time.time()
            if current_time - self._last_checkpoint_time >= self._checkpoint_interval:
                self._create_checkpoint_now()

        def _create_checkpoint_now(self):
            """立即创建检查点"""
            try:
                # 获取Actor状态
                state_data = self._get_state_for_checkpoint()

                # 创建检查点
                checkpoint_id = self._checkpoint_manager.create_checkpoint(
                    str(ray.get_runtime_context().get_actor_id()),
                    'actor',
                    state_data
                )

                self._last_checkpoint_time = time.time()
                return checkpoint_id

            except Exception as e:
                logger.error(f"Failed to create checkpoint: {e}")
                return None

        # 替换类方法
        actor_class.__init__ = __init__
        actor_class.save_checkpoint = save_checkpoint
        actor_class.restore_checkpoint = restore_checkpoint
        actor_class._start_checkpoint_thread = _start_checkpoint_thread
        actor_class._check_and_create_checkpoint = _check_and_create_checkpoint
        actor_class._create_checkpoint_now = _create_checkpoint_now

        return actor_class

    return decorator
```

## 总结

Ray的错误处理和容错机制通过多层次的设计实现了高可靠性：

1. **分层容错**：从应用到集群的全栈容错策略
2. **故障检测**：基于心跳和超时的智能故障检测
3. **自动恢复**：任务重试、Actor重启等自动恢复机制
4. **检查点机制**：支持状态恢复的检查点系统
5. **策略配置**：灵活的容错策略配置
6. **监控告警**：完善的故障监控和告警机制

这种设计使得Ray能够在复杂的分布式环境中提供稳定可靠的计算服务，为AI和机器学习工作负载提供了坚实的容错保障。

## 参考源码路径

- 容错管理：`/ray/python/ray/_private/fault_tolerance.py`
- 故障检测：`/ray/python/ray/_private/failure_detector.py`
- 恢复管理：`/ray/python/ray/_private/recovery_manager.py`
- 检查点系统：`/ray/python/ray/_private/checkpoint_manager.py`
- 重试策略：`/ray/python/ray/_private/retry_policy.py`
- 核心C++实现：`/src/ray/gcs/gcs_server.cc`
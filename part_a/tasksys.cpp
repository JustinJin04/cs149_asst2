#include "tasksys.h"

#include "CycleTimer.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
  return 0;
}

void TaskSystemSerial::sync() { return; }

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  _max_num_threads = num_threads-1;
  _mutex = new std::mutex();
  _thread_pool = new std::thread[_max_num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
  delete _mutex;
  delete[] _thread_pool;
}

void TaskSystemParallelSpawn::threadRun(int* task_id, int num_total_tasks,
                                        IRunnable* runnable) {
  while (1) {
    _mutex->lock();
    if (*task_id >= num_total_tasks) {
      _mutex->unlock();
      break;
    }
    int my_task_id = *task_id;
    *task_id = my_task_id + 1;
    _mutex->unlock();
    runnable->runTask(my_task_id, num_total_tasks);
  }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  int task_id = 0;
  for (int i = 0; i < _max_num_threads; ++i) {
    _thread_pool[i] = std::thread(&TaskSystemParallelSpawn::threadRun, this,
                                  &task_id, num_total_tasks, runnable);
  }
  threadRun(&task_id, num_total_tasks, runnable);

  for (int i = 0; i < _max_num_threads; ++i) {
    _thread_pool[i].join();
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  return 0;
}

void TaskSystemParallelSpawn::sync() { return; }

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  _num_worker = num_threads - 1;
  _worker_pool = new Worker[_num_worker];
  for (int i = 0; i < _num_worker; ++i) {
    _worker_pool[i].initialize(i);
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  for (int i = 0; i < _num_worker; ++i) {
    _worker_pool[i].exit();
  }
  delete[] _worker_pool;
  printf("system exit\n");
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable,
                                               int num_total_tasks) {
  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  _next_task_id.store(0);
  _finish_worker_num.store(0);
  for (int i = 0; i < _num_worker; ++i) {
    _worker_pool[i].start_working(&_next_task_id, num_total_tasks, runnable,
                                  &_finish_worker_num);
  }

  // main thread also do some work
  while (1) {
    int task_id = _next_task_id.fetch_add(1);
    if (task_id >= num_total_tasks) {
      break;
    }
    runnable->runTask(task_id, num_total_tasks);
  }

  // wait for other worker to finish
  // printf("start to wait\n");
  while (_finish_worker_num.load() < _num_worker);
  // printf("finish waiting\n");
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() { return; }

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  _num_worker = num_threads;
  _worker_pool = new Worker[_num_worker];
  for(int i=0;i<_num_worker;++i){
    _worker_pool[i].initialize();
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  for(int i=0;i<_num_worker;++i){
    _worker_pool[i].exit();
  }
  delete[] _worker_pool;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable,
                                               int num_total_tasks) {
  //
  // TODO: CS149 students will modify the implementation of this
  // method in Parts A and B.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  std::atomic<int> nxt_task_id{0};
  for(int i=0;i<_num_worker;++i){
    _worker_pool[i].start_working(&nxt_task_id, num_total_tasks, runnable);
  }

  for(int i=0;i<_num_worker;++i){
    _worker_pool[i].wait();
  }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}

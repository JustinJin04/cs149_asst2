#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "itasksys.h"
#include<cstdio>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
 public:
  TaskSystemSerial(int num_threads);
  ~TaskSystemSerial();
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
 public:
  TaskSystemParallelSpawn(int num_threads);
  ~TaskSystemParallelSpawn();
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();

 private:
  int _max_num_threads;
  std::mutex* _mutex;
  std::thread* _thread_pool;

  void threadRun(int* task_idx, int num_total_task, IRunnable* runnable);
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
 public:
  TaskSystemParallelThreadPoolSpinning(int num_threads);
  ~TaskSystemParallelThreadPoolSpinning();
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();

 private:
  class Worker {
   public:
    void initialize(int id) {
      _has_work.store(false);
      _can_exit.store(false);
      _thread = std::thread(
          &TaskSystemParallelThreadPoolSpinning::Worker::_threadRun, this, id);
    }

    void start_working(std::atomic<int>* next_task_id, int num_total_work,
                       IRunnable* runnable,
                       std::atomic<int>* finish_worker_num) {
      _next_task_id_ptr = next_task_id;
      _num_total_work = num_total_work;
      _runnable = runnable;
      _finish_worker_num_ptr = finish_worker_num;

      _has_work.store(true);
    }

    void exit() {
      _can_exit.store(true);
      _thread.join();
    }

   private:
    std::thread _thread;
    std::atomic<bool> _has_work;
    std::atomic<bool> _can_exit;

    std::atomic<int>* _next_task_id_ptr;
    std::atomic<int>* _finish_worker_num_ptr;

    int _num_total_work;
    IRunnable* _runnable;

    void _threadRun(int id) {
      printf("hello from %d\n", id);
      while (1) {
        if (_can_exit.load()) {
          return;
        }
        if (!_has_work.load()) {
          continue;
        }

        int task_id = _next_task_id_ptr->fetch_add(1);
        printf("taskid=%d from %d\n", task_id, id);

        if (task_id >= _num_total_work) {
          _has_work.store(false);
          int finish = _finish_worker_num_ptr->fetch_add(1);
          printf("finish workers: %d from %d\n", finish, id);
          continue;
        }
        _runnable->runTask(task_id, _num_total_work);
      }
    }
  };

  Worker* _worker_pool;
  int _num_worker;

  std::atomic<int> _next_task_id;
  std::atomic<int> _finish_worker_num;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
 public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char* name();
  void run(IRunnable* runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                          const std::vector<TaskID>& deps);
  void sync();
};

#endif

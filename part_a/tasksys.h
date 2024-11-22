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

    bool has_work(){
      return _has_work.load();
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
      while (1) {
        if (_can_exit.load()) {
          return;
        }
        if (!_has_work.load()) {
          continue;
        }

        int task_id = _next_task_id_ptr->fetch_add(1);
        if (task_id >= _num_total_work) {
          // printf("%d worker finish work\n", id);
          _has_work.store(false);
          _finish_worker_num_ptr->fetch_add(1);
          continue;
        }
        // printf("task %d gives to %d. total: %d\n", task_id, id, _num_total_work);
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

 private:
  class Worker {
   public:
    void initialize(){
      _thread = std::thread(&TaskSystemParallelThreadPoolSleeping::Worker::_threadRun, this);
    }
    void start_working(std::atomic<int>* nxt_id, int num_total_tasks, IRunnable* runnable){
      _nxt_id_ptr = nxt_id;
      _num_total_tasks = num_total_tasks;
      _runnable = runnable;
      
      std::unique_lock<std::mutex> lk(_mutex);
      _has_work = true;
      _worker_cv.notify_one();
    }

    void wait(){
      std::unique_lock<std::mutex> lk(_mutex);
      while(_has_work){
        _host_cv.wait(lk);
      }
    }

    void exit(){
      std::unique_lock<std::mutex> lk(_mutex);
      _can_exit = true;
      _worker_cv.notify_one();
      lk.unlock();
      _thread.join();
    }

   private:
    std::mutex _mutex;
    std::condition_variable _host_cv;
    std::condition_variable _worker_cv;
    bool _has_work{false};
    bool _can_exit{false};
    
    std::atomic<int>* _nxt_id_ptr;
    int _num_total_tasks;
    IRunnable* _runnable;

    std::thread _thread;
    void _threadRun(){
      while(1){
        std::unique_lock<std::mutex> lk(_mutex);
        while(!_has_work && !_can_exit){
          _worker_cv.wait(lk);
        }
        if(_can_exit){
          return;
        }
        // has work
        int task_id = _nxt_id_ptr->fetch_add(1);
        if(task_id >= _num_total_tasks){
          _has_work = false;
          _host_cv.notify_one();
          lk.unlock();
          continue;
        }
        lk.unlock();
        _runnable->runTask(task_id, _num_total_tasks);
      }
    }
  };

  Worker* _worker_pool;
  int _num_worker;
};

#endif

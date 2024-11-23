#ifndef _TASKSYS_H
#define _TASKSYS_H

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <queue>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "itasksys.h"

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
  struct SubTask {
    TaskID task_id;
    IRunnable* runnable;
    int sub_id;
    int total_num;

    SubTask(TaskID _task_id, IRunnable* _runnable, int _sub_id,
            int _total_num) {
      task_id = _task_id;
      runnable = _runnable;
      sub_id = _sub_id;
      total_num = _total_num;
    }
  };

  struct Task {
    TaskID id;
    IRunnable* runnable;
    int total_num;
    std::vector<TaskID> deps;

    Task(TaskID _id, IRunnable* _runnable, int _total_num,
         std::vector<TaskID> _deps) {
      id = _id;
      runnable = _runnable;
      total_num = _total_num;
      deps = _deps;
    }
  };

  std::queue<SubTask> _ready_queue;
  std::mutex _ready_queue_mutex;

  std::condition_variable _host_cv;
  std::vector<int> _remain_num_vec;
  TaskID _max_finished_id{-1};
  std::mutex _remain_num_mutex;

  std::atomic<bool> _can_exit{false};
  TaskID _nxt_id{0};

  std::queue<Task> _task_queue;
  std::mutex _task_queue_mutex;

  std::thread* _thread_pool;
  int _num_thread;
  void _threadRun() {
    while (1) {
      if (_can_exit.load()) {
        return;
      }
      // first check ready queue
      std::unique_lock<std::mutex> ready_lk(_ready_queue_mutex);
      while (!_ready_queue.empty()) {
        SubTask subtask = _ready_queue.front();
        _ready_queue.pop();
        ready_lk.unlock();
        subtask.runnable->runTask(subtask.sub_id, subtask.total_num);

        std::unique_lock<std::mutex> remain_lk(_remain_num_mutex);
        _remain_num_vec[subtask.task_id] -= 1;
        if (_remain_num_vec[subtask.task_id] == 0 &&
            subtask.task_id == _max_finished_id + 1) {
          _max_finished_id = subtask.task_id;
          for (int i = _max_finished_id + 1; i < (int)_remain_num_vec.size();
               ++i) {
            if (_remain_num_vec[i] == 0) {
              _max_finished_id += 1;
            } else {
              break;
            }
          }
          _host_cv.notify_one();
        }
        remain_lk.unlock();

        ready_lk.lock();
      }
      ready_lk.unlock();

      // then check the task queue and transfer to ready queue if deps are met
      std::unique_lock<std::mutex> task_lk(_task_queue_mutex);
      while (!_task_queue.empty()) {
        Task task = _task_queue.front();
        std::unique_lock<std::mutex> remain_lk(_remain_num_mutex);
        bool is_ready = true;
        for (auto id : task.deps) {
          if (_remain_num_vec[id] != 0) {
            is_ready = false;
            break;
          }
        }
        remain_lk.unlock();

        if(!is_ready){
          break;
        } else {
          _task_queue.pop();
          ready_lk.lock();
          for (int i = 0; i < task.total_num; ++i) {
            _ready_queue.push(
                SubTask(task.id, task.runnable, i, task.total_num));
          }
          ready_lk.unlock();
        }

      }
      task_lk.unlock();
    }
  }
};

#endif

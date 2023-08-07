use std::future::Future;
use std::sync::{Arc, Mutex};
use async_task::{Runnable, Task};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use log::warn;

pub struct BoundedRunner {
  task_tracker: TaskTracker,
  receiver: Receiver<Runnable>,
}

pub struct BoundedSpawner {
  task_tracker: TaskTracker,
  sender: Sender<Runnable>,
}

pub struct BoundedExecutor {
  runner: BoundedRunner,
  spawner: BoundedSpawner,
}

impl BoundedExecutor {
  pub fn new(queue_size: usize) -> Self {
    let task_tracker = TaskTracker::new(queue_size);
    let (sender, receiver) = bounded(queue_size);
    let runner = BoundedRunner {
      task_tracker: task_tracker.clone(),
      receiver,
    };
    let spawner = BoundedSpawner {
      task_tracker,
      sender,
    };
    Self { runner, spawner }
  }

  pub fn split(self) -> (BoundedRunner, BoundedSpawner) {
    (self.runner, self.spawner)
  }
}

impl BoundedSpawner {
  pub fn spawn<F>(&self, future: F) -> Result<(), SpawnError>
    where
        F: Future<Output=()> + Send + 'static
  {
    let sender = self.sender.clone();
    let schedule = move |runnable| {
      match sender.try_send(runnable) {
        Ok(_) => {},
        Err(TrySendError::Full(_)) => unreachable!("Should not be possible"),
        Err(TrySendError::Disconnected(_)) => {
          warn!("run loop shut down, scheduling is ignored...")
        }
      }
    };

    let (runnable, task) = async_task::spawn(future, schedule);

    match self.task_tracker.try_push(task) {
      Ok(_) => {
        runnable.schedule();
        Ok(())
      },
      Err(_) => {
        // Dropping the task canceled it, so we're good to just return an error...
        Err(SpawnError::QueueFull)
      },
    }
  }
}

impl BoundedRunner {
  pub fn run_loop(self) {
    for runnable in self.receiver {
      // If run() told us whether the task completed then we could actually just
      // track total outstanding tasks with an AtomicUsize and save a lot of
      // resources/complexity.
      if !runnable.run() {
        self.task_tracker.remove_all_finished_tasks();
      }
    }
  }
}

#[derive(Clone)]
struct TaskTracker {
  unfinished_tasks: Arc<Mutex<Vec<TaskRecord>>>,
  max_tasks: usize,
}

struct TaskRecord(Task<()>);

impl TaskTracker {
  pub fn new(max_tasks: usize) -> Self {
    Self {
      unfinished_tasks: Arc::new(Mutex::new(Vec::with_capacity(max_tasks))),
      max_tasks,
    }
  }

  pub fn try_push(&self, task: Task<()>) -> Result<(), ()> {
    let mut unfinished_tasks = self.unfinished_tasks.lock().unwrap();
    if unfinished_tasks.len() >= self.max_tasks {
      Err(())
    } else {
      unfinished_tasks.push(TaskRecord(task));
      Ok(())
    }
  }

  pub fn remove_all_finished_tasks(&self) {
    let mut unfinished_tasks = self.unfinished_tasks.lock().unwrap();
    unfinished_tasks.retain(|t| !t.0.is_finished());
  }
}

#[derive(Debug)]
pub enum SpawnError {
  // Queue is currently full, caller may try again after we've polled existing spawned
  // futures to completion.
  QueueFull,
}
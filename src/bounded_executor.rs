use std::future::Future;
use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, Ordering};
use async_task::{Runnable};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use log::warn;

pub struct BoundedRunner {
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
    if !self.task_tracker.prepare_to_spawn() {
      return Err(SpawnError::QueueFull);
    }

    let tracker = self.task_tracker.clone();
    let future_wrapper = async move {
      let result = future.await;
      tracker.mark_completion();
      result
    };

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

    let (runnable, task) = async_task::spawn(future_wrapper, schedule);
    runnable.schedule();
    task.detach();

    Ok(())
  }
}

impl BoundedRunner {
  pub fn run_loop(self) {
    for runnable in self.receiver {
      runnable.run();
    }
  }
}

#[derive(Clone)]
struct TaskTracker {
  unfinished_tasks: Arc<AtomicUsize>,
  max_tasks: usize,
}

impl TaskTracker {
  pub fn new(max_tasks: usize) -> Self {
    Self {
      unfinished_tasks: Arc::new(AtomicUsize::new(0)),
      max_tasks,
    }
  }

  pub fn prepare_to_spawn(&self) -> bool {
    let previous = self.unfinished_tasks.fetch_add(1, Ordering::Relaxed);
    if previous >= self.max_tasks {
      self.unfinished_tasks.fetch_sub(1, Ordering::Relaxed);
      false
    } else {
      true
    }
  }

  pub fn mark_completion(&self) {
    self.unfinished_tasks.fetch_sub(1, Ordering::Relaxed);
  }
}

#[derive(Debug)]
pub enum SpawnError {
  // Queue is currently full, caller may try again after we've polled existing spawned
  // futures to completion.
  QueueFull,
}
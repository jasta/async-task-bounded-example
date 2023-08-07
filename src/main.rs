use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};
use atomic_waker::AtomicWaker;
use log::{debug, info, trace};
use rand::Rng;
use crate::bounded_executor::{BoundedExecutor, BoundedSpawner};

mod bounded_executor;

const SPAWN_BACKOFF: Duration = Duration::from_millis(1);
const TASK_SLEEP_MIN: Duration = Duration::from_millis(0);
const TASK_SLEEP_MAX: Duration = Duration::from_millis(5);

const PRINT_NTH_TASK: usize = 1000;

fn main() -> anyhow::Result<()> {
  env_logger::init();
  let (runner, spawner) = BoundedExecutor::new(4).split();
  let spawner = Arc::new(spawner);
  let test = StressTest::new();
  spawner.spawn(test.run_stress(spawner.clone())).unwrap();
  runner.run_loop();
  Ok(())
}

struct StressTest {
  next_token: usize,
  total_spawned: usize,
  total_errors: usize,
}

impl StressTest {
  pub fn new() -> Self {
    Self {
      next_token: 0,
      total_spawned: 0,
      total_errors: 0,
    }
  }
}

impl StressTest {
  pub async fn run_stress(mut self, spawner: Arc<BoundedSpawner>) {
    let mut next_task = self.next_task();
    loop {
      match spawner.spawn(next_task.clone().run()) {
        Ok(_) => {
          self.total_spawned += 1;
          if self.total_spawned > 0 && self.total_spawned % PRINT_NTH_TASK == 0 {
            info!(
                "Spawned {} total tasks with {} spawn errors...",
                self.total_spawned,
                self.total_errors);
          }
          debug!("Spawned {next_task:?}!");
          next_task = self.next_task();
        }
        Err(e) => {
          self.total_errors += 1;
          debug!("Error spawning {next_task:?}: {e:?}, sleeping for {} ms...", SPAWN_BACKOFF.as_millis());
          Sleep::new(SPAWN_BACKOFF).await;
        }
      }
    }
  }

  pub fn next_task(&mut self) -> StressTask {
    let current_token = self.next_token;
    self.next_token += 1;

    let sleep = rand::thread_rng().gen_range(TASK_SLEEP_MIN..=TASK_SLEEP_MAX);

    StressTask {
      token: current_token,
      sleep,
    }
  }
}

#[derive(Debug, Clone)]
struct StressTask {
  token: usize,
  sleep: Duration,
}

impl StressTask {
  pub async fn run(self) {
    let token = self.token;
    trace!("[{token}] sleeping for {} ms...", self.sleep.as_millis());
    Sleep::new(self.sleep).await;
    trace!("[{token}] finished!");
  }
}

struct Sleep {
  wake_at: Instant,
  waker: Arc<AtomicWaker>,
  has_thread: AtomicBool,
}

impl Sleep {
  pub fn new(duration: Duration) -> Self {
    Self {
      wake_at: Instant::now() + duration,
      waker: Arc::new(AtomicWaker::new()),
      has_thread: AtomicBool::new(false),
    }
  }
}

impl Future for Sleep {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    self.waker.register(cx.waker());

    if !self.has_thread.fetch_and(true, Ordering::Relaxed) {
      let wake_at = self.wake_at;
      let waker = self.waker.clone();
      thread::spawn(move || {
        let delay = wake_at - Instant::now();
        thread::sleep(delay);

        waker.wake();
      });
    }

    if Instant::now() >= self.wake_at {
      Poll::Ready(())
    } else {
      Poll::Pending
    }
  }
}
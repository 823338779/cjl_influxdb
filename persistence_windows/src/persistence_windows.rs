//!  In memory structures for tracking data ingest and when persistence can or should occur.
use std::{
    collections::{btree_map::Entry, BTreeMap, VecDeque},
    num::NonZeroUsize,
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use time::{Time, TimeProvider};

use data_types::{
    partition_metadata::PartitionAddr, sequence::Sequence, write_summary::WriteSummary,
};
use internal_types::freezable::{Freezable, FreezeHandle};

use crate::min_max_sequence::MinMaxSequence;
use crate::{checkpoint::PartitionCheckpoint, min_max_sequence::OptionalMinMaxSequence};

const DEFAULT_CLOSED_WINDOW: Duration = Duration::from_secs(30);

/// PersistenceWindows keep track of ingested data within a partition to determine when it
/// can be persisted. This allows IOx to receive out of order writes (in their timestamps) while
/// persisting mostly in non-time overlapping Parquet files.
///
/// The sequencer_id in the code below will map to a Kafka partition id. The sequence_number maps
/// to a Kafka offset. Because IOx will run without Kafka, we use the more generic terms rather
/// than the Kafka terminology.
///
/// The `PersistenceWindows` operate on two different types of time
///
/// * row timestamps - these are the row's value for the `time` column
/// * Wall timestamps - these are the Wall clock of the system used to determine
///   the "age" of a set of writes within a PersistenceWindow
///
/// To aid testing Wall timestamps are passed to many methods instead of directly using `Utc::now`
///
/// The PersistenceWindows answer the question: - "What is the maximum row timestamp in the writes
/// that arrived more than late_arrival_period seconds ago, as determined by wall clock time"
#[derive(Debug)]
pub struct PersistenceWindows {
    persistable: Freezable<Option<Window>>,
    closed: VecDeque<Window>,
    open: Option<Window>,

    addr: PartitionAddr,

    late_arrival_period: Duration,

    closed_window_period: Duration,

    /// The instant this PersistenceWindows was created
    #[allow(dead_code)]
    time_of_first_write: Time,

    /// The maximum Wall timestamp that has been passed to PersistenceWindows::add_range
    time_of_last_write: Time,

    /// maps sequencer_id to the maximum sequence passed to PersistenceWindows::add_range
    max_sequence_numbers: BTreeMap<u32, u64>,

    time_provider: Arc<dyn TimeProvider>,
}

/// A handle for flushing data from the `PersistenceWindows`
///
/// When a `FlushHandle` is created it computes the row timestamp that should be persisted up to
///
/// It then allows flushing the corresponding writes from the `PersistenceWindows` that were
/// present at the time the `FlushHandle` was created. Even if later writes have been recorded
/// in the `PersistenceWindows` in the intervening time
///
#[derive(Debug)]
pub struct FlushHandle {
    handle: FreezeHandle,
    /// The number of closed windows at the time of the handle's creation
    ///
    /// This identifies the windows that can have their
    /// minimum timestamps truncated on flush
    closed_count: usize,

    /// The address of the partition
    addr: PartitionAddr,

    /// The row timestamp to flush
    timestamp: Time,

    /// The sequence number ranges not including those persisted by this flush
    sequencer_numbers: BTreeMap<u32, OptionalMinMaxSequence>,
}

impl FlushHandle {
    /// Should flush all rows with a timestamp less than or equal to this
    pub fn timestamp(&self) -> Time {
        self.timestamp
    }

    /// Returns a partition checkpoint that describes the state of this partition
    /// after the flush
    pub fn checkpoint(&self) -> PartitionCheckpoint {
        PartitionCheckpoint::new(
            Arc::clone(&self.addr.table_name),
            Arc::clone(&self.addr.partition_key),
            self.sequencer_numbers.clone(),
            self.timestamp,
        )
    }
}

impl PersistenceWindows {
    pub fn new(
        addr: PartitionAddr,
        late_arrival_period: Duration,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let closed_window_period = late_arrival_period.min(DEFAULT_CLOSED_WINDOW);
        let closed_window_count = late_arrival_period.as_secs() / closed_window_period.as_secs();

        let now = time_provider.now();

        Self {
            persistable: Freezable::new(None),
            closed: VecDeque::with_capacity(closed_window_count as usize),
            open: None,
            addr,
            late_arrival_period,
            closed_window_period,
            time_of_first_write: now,
            time_of_last_write: now,
            max_sequence_numbers: Default::default(),
            time_provider,
        }
    }

    /// Updates the late arrival period of this `PersistenceWindows` instance
    pub fn set_late_arrival_period(&mut self, late_arrival_period: Duration) {
        self.closed_window_period = late_arrival_period.min(DEFAULT_CLOSED_WINDOW);
        self.late_arrival_period = late_arrival_period;
    }

    /// Marks sequence numbers as seen and persisted.
    ///
    /// This can be used during replay to keep in-memory information in sync with the already persisted data.
    pub fn mark_seen_and_persisted(&mut self, partition_checkpoint: &PartitionCheckpoint) {
        for (sequencer_id, min_max) in partition_checkpoint.sequencer_numbers_iter() {
            match self.max_sequence_numbers.entry(sequencer_id) {
                Entry::Occupied(mut occupied) => {
                    *occupied.get_mut() = (*occupied.get()).max(min_max.max());
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(min_max.max());
                }
            }
        }
    }

    /// Updates the windows with the information from a batch of rows from a single sequencer
    /// to the same partition. `min_time` and `max_time` are row timestamps in the written data
    ///
    /// The times passed in are used to determine where to split the in-memory data when persistence
    /// is triggered (either by crossing a row count threshold or time).
    ///
    /// TODO: Use WriteSummary
    ///
    /// # Panics
    /// - When `min_time > max_time`.
    pub fn add_range(
        &mut self,
        sequence: Option<&Sequence>,
        row_count: NonZeroUsize,
        min_time: Time,
        max_time: Time,
    ) {
        // TimeProvider is not monotonic
        let time_of_write = self.time_of_last_write.max(self.time_provider.now());
        assert!(
            min_time <= max_time,
            "PersistenceWindows::add_range called with min_time ({}) > max_time ({})",
            min_time,
            max_time
        );
        self.time_of_last_write = time_of_write;

        if let Some(sequence) = sequence {
            match self.max_sequence_numbers.entry(sequence.id) {
                Entry::Occupied(mut occupied) => {
                    assert!(
                        *occupied.get() < sequence.number,
                        "sequence number {} for sequencer {} was not greater than previous {}",
                        sequence.number,
                        sequence.id,
                        *occupied.get()
                    );
                    *occupied.get_mut() = sequence.number;
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(sequence.number);
                }
            }
        }

        self.rotate();

        match self.open.as_mut() {
            Some(w) => w.add_range(sequence, row_count, min_time, max_time, time_of_write),
            None => {
                self.open = Some(Window::new(
                    time_of_write,
                    sequence,
                    row_count,
                    min_time,
                    max_time,
                ))
            }
        };
    }

    /// rotates open window to closed if past time and any closed windows to persistable.
    fn rotate(&mut self) {
        self.rotate_impl(self.time_provider.now())
    }

    fn rotate_impl(&mut self, now: Time) {
        let rotate = self
            .open
            .as_ref()
            .map(|w| w.is_closeable(now, self.closed_window_period))
            .unwrap_or(false);

        if rotate {
            self.closed.push_back(self.open.take().unwrap())
        }

        let late_arrival_period = self.late_arrival_period;

        // if there is no ongoing persistence operation, try and
        // add closed windows to the `persistable` window
        if let Some(mut persistable) = self.persistable.get_mut() {
            while self
                .closed
                .front()
                .map(|w| w.is_persistable(now, late_arrival_period))
                .unwrap_or(false)
            {
                let w = self.closed.pop_front().unwrap();
                match persistable.as_mut() {
                    Some(persistable_window) => persistable_window.add_window(w),
                    None => *persistable = Some(w),
                }
            }
        }
    }

    /// Returns the sequence number range of unpersisted writes described by this instance.
    pub fn sequencer_numbers(&self) -> BTreeMap<u32, OptionalMinMaxSequence> {
        self.sequencer_numbers_inner(false)
    }

    /// Returns the sequence number range of unpersisted writes described by this instance
    ///
    /// Can optionally skip the persistable window if any.
    fn sequencer_numbers_inner(
        &self,
        skip_persistable: bool,
    ) -> BTreeMap<u32, OptionalMinMaxSequence> {
        if self.is_empty() {
            Default::default()
        }

        let (skip, flush_time) = match (skip_persistable, self.persistable.deref()) {
            (true, Some(persistable)) => (1, Some(persistable.max_time)),
            _ => (0, None),
        };

        self.max_sequence_numbers
            .iter()
            .map(|(sequencer_id, max_sequence_number)| {
                // Find first window containing writes from sequencer_id
                let window = self
                    .windows()
                    .skip(skip)
                    .filter_map(|window| {
                        if let Some(flush_time) = flush_time {
                            if window.max_time <= flush_time {
                                return None;
                            }
                        }
                        window.sequencer_numbers.get(sequencer_id)
                    })
                    .next();

                let min = window.map(|window| {
                    assert!(window.max() <= *max_sequence_number);
                    window.min()
                });

                (
                    *sequencer_id,
                    OptionalMinMaxSequence::new(min, *max_sequence_number),
                )
            })
            .collect()
    }

    /// Acquire a handle that flushes all unpersisted data
    pub fn flush_all_handle(&mut self) -> Option<FlushHandle> {
        self.flush_handle_impl(Time::MAX)
    }

    /// Acquire a handle that prevents mutation of the persistable window until dropped
    ///
    /// Returns `None` if there is an outstanding handle or nothing to persist
    pub fn flush_handle(&mut self) -> Option<FlushHandle> {
        self.flush_handle_impl(self.time_provider.now())
    }

    fn flush_handle_impl(&mut self, now: Time) -> Option<FlushHandle> {
        // Verify no active flush handles before closing open window
        self.persistable.get_mut()?;

        // Close current open window if any
        if let Some(open) = self.open.take() {
            self.closed.push_back(open)
        }

        // Rotate into persistable window
        self.rotate_impl(now);

        Some(FlushHandle {
            handle: self.persistable.try_freeze()?,
            closed_count: self.closed.len(),
            addr: self.addr.clone(),
            timestamp: self.persistable.as_ref()?.max_time,
            sequencer_numbers: self.sequencer_numbers_inner(true),
        })
    }

    /// Clears out the persistable window
    pub fn flush(&mut self, handle: FlushHandle) {
        let closed_count = handle.closed_count;
        let timestamp = handle.timestamp;

        assert!(
            self.closed.len() >= closed_count,
            "windows dropped from closed whilst locked"
        );

        let persistable = self
            .persistable
            .unfreeze(handle.handle)
            .take()
            .expect("expected persistable window");

        assert_eq!(
            persistable.max_time, timestamp,
            "persistable max time doesn't match handle"
        );

        // Everything up to and including persistable max time will have been persisted
        if let Some(new_min) = persistable.max_time.checked_add(Duration::from_nanos(1)) {
            for w in self.closed.iter_mut().take(closed_count) {
                if w.min_time < new_min {
                    w.min_time = new_min;
                }
            }

            // Drop any now empty windows
            let mut tail = self.closed.split_off(closed_count);
            self.closed.retain(|w| w.max_time >= new_min);
            self.closed.append(&mut tail);
        } else {
            // drop all windows (persisted everything)
            self.closed.clear();
        }
    }

    /// Returns an iterator over the windows starting with the oldest
    fn windows(&self) -> impl Iterator<Item = &Window> {
        self.persistable
            .as_ref()
            .into_iter()
            .chain(self.closed.iter())
            .chain(self.open.as_ref().into_iter())
    }

    /// Returns the minimum window
    fn minimum_window(&self) -> Option<&Window> {
        self.windows().next()
    }

    /// Returns approximate summaries of the unpersisted writes contained
    /// recorded by this PersistenceWindow instance
    ///
    /// These are approximate because persistence may partially flush a window, which will
    /// update the min row timestamp but not the row count
    pub fn summaries(&self) -> impl Iterator<Item = WriteSummary> + '_ {
        self.windows().map(move |window| WriteSummary {
            time_of_first_write: window.time_of_first_write,
            time_of_last_write: window.time_of_last_write,
            min_timestamp: window.min_time,
            max_timestamp: window.max_time,
            row_count: window.row_count.get(),
        })
    }

    /// Returns true if this PersistenceWindows instance is empty
    pub fn is_empty(&self) -> bool {
        self.minimum_window().is_none()
    }

    /// Returns the unpersisted sequencer numbers that represent the min
    pub fn minimum_unpersisted_sequence(&self) -> Option<BTreeMap<u32, MinMaxSequence>> {
        self.minimum_window().map(|x| x.sequencer_numbers.clone())
    }

    /// Returns the minimum unpersisted age
    pub fn minimum_unpersisted_age(&self) -> Option<Time> {
        self.minimum_window().map(|x| x.time_of_first_write)
    }

    /// Returns the minimum unpersisted timestamp
    pub fn minimum_unpersisted_timestamp(&self) -> Option<Time> {
        self.windows().map(|x| x.min_time).min()
    }

    /// Returns the maximum unpersisted timestamp
    pub fn maximum_unpersisted_timestamp(&self) -> Option<Time> {
        self.windows().map(|x| x.max_time).max()
    }

    /// Returns the number of persistable rows
    pub fn persistable_row_count(&self) -> usize {
        let now = self.time_provider.now();
        self.windows()
            .take_while(|window| window.is_persistable(now, self.late_arrival_period))
            .map(|window| window.row_count.get())
            .sum()
    }
}

#[derive(Debug, Clone)]
struct Window {
    /// The server time when this window was created. Used to determine how long data in this
    /// window has been sitting in memory.
    time_of_first_write: Time,
    /// The server time of the last write to this window
    time_of_last_write: Time,
    /// The number of rows in the window
    row_count: NonZeroUsize,
    /// min time value for data in the window
    min_time: Time,
    /// max time value for data in the window
    max_time: Time,
    /// maps sequencer_id to the minimum and maximum sequence numbers seen
    sequencer_numbers: BTreeMap<u32, MinMaxSequence>,
}

impl Window {
    fn new(
        time_of_write: Time,
        sequence: Option<&Sequence>,
        row_count: NonZeroUsize,
        min_time: Time,
        max_time: Time,
    ) -> Self {
        let mut sequencer_numbers = BTreeMap::new();
        if let Some(sequence) = sequence {
            sequencer_numbers.insert(
                sequence.id,
                MinMaxSequence::new(sequence.number, sequence.number),
            );
        }

        Self {
            time_of_first_write: time_of_write,
            time_of_last_write: time_of_write,
            row_count,
            min_time,
            max_time,
            sequencer_numbers,
        }
    }

    /// Updates the window with the passed in range. This function assumes that sequence numbers
    /// are always increasing.
    fn add_range(
        &mut self,
        sequence: Option<&Sequence>,
        row_count: NonZeroUsize,
        min_time: Time,
        max_time: Time,
        time_of_write: Time,
    ) {
        assert!(self.time_of_first_write <= time_of_write);
        assert!(self.time_of_last_write <= time_of_write);
        self.time_of_last_write = time_of_write;

        self.row_count =
            NonZeroUsize::new(self.row_count.get() + row_count.get()).expect("both are > 0");
        if self.min_time > min_time {
            self.min_time = min_time;
        }
        if self.max_time < max_time {
            self.max_time = max_time;
        }
        if let Some(sequence) = sequence {
            match self.sequencer_numbers.get_mut(&sequence.id) {
                Some(n) => {
                    assert!(sequence.number > n.max());
                    *n = MinMaxSequence::new(n.min(), sequence.number);
                }
                None => {
                    self.sequencer_numbers.insert(
                        sequence.id,
                        MinMaxSequence::new(sequence.number, sequence.number),
                    );
                }
            }
        }
    }

    /// Add one window to another. Used to collapse closed windows into persisted.
    fn add_window(&mut self, other: Self) {
        assert!(self.time_of_last_write <= other.time_of_first_write);
        assert!(self.time_of_last_write <= other.time_of_last_write);

        self.time_of_last_write = other.time_of_last_write;
        self.row_count =
            NonZeroUsize::new(self.row_count.get() + other.row_count.get()).expect("both are > 0");
        if self.min_time > other.min_time {
            self.min_time = other.min_time;
        }
        if self.max_time < other.max_time {
            self.max_time = other.max_time;
        }
        for (sequencer_id, other_n) in other.sequencer_numbers {
            match self.sequencer_numbers.get_mut(&sequencer_id) {
                Some(n) => {
                    assert!(other_n.max() > n.max());
                    *n = MinMaxSequence::new(n.min(), other_n.max());
                }
                None => {
                    self.sequencer_numbers.insert(sequencer_id, other_n);
                }
            }
        }
    }

    /// If this window can be closed
    fn is_closeable(&self, now: Time, closed_window_period: Duration) -> bool {
        now.checked_duration_since(self.time_of_first_write)
            .map(|x| x >= closed_window_period)
            .unwrap_or(false)
    }

    /// If this window is persistable
    fn is_persistable(&self, now: Time, late_arrival_period: Duration) -> bool {
        now.checked_duration_since(self.time_of_first_write)
            .map(|x| x >= late_arrival_period)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use time::MockProvider;

    use super::*;

    fn make_windows(
        late_arrival_period: Duration,
        start: Time,
    ) -> (PersistenceWindows, Arc<MockProvider>) {
        let provider = Arc::new(MockProvider::new(start));
        let windows = PersistenceWindows::new(
            PartitionAddr {
                db_name: Arc::from("db"),
                table_name: Arc::from("table_name"),
                partition_key: Arc::from("partition_key"),
            },
            late_arrival_period,
            Arc::<MockProvider>::clone(&provider),
        );
        (windows, provider)
    }

    #[test]
    fn time_go_backwards() {
        let (mut w, time) = make_windows(Duration::from_secs(60), Time::from_timestamp_nanos(0));

        time.set(Time::from_timestamp_nanos(1));
        w.add_range(
            Some(&Sequence { id: 1, number: 1 }),
            NonZeroUsize::new(1).unwrap(),
            Time::from_timestamp_nanos(100),
            Time::from_timestamp_nanos(200),
        );

        time.set(Time::from_timestamp_nanos(0));
        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(1).unwrap(),
            Time::from_timestamp_nanos(100),
            Time::from_timestamp_nanos(200),
        );
    }

    #[test]
    #[should_panic(expected = "PersistenceWindows::add_range called with min_time")]
    fn panics_when_min_time_gt_max_time() {
        let (mut w, _) = make_windows(Duration::from_secs(60), Time::from_timestamp_nanos(0));

        w.add_range(
            Some(&Sequence { id: 1, number: 1 }),
            NonZeroUsize::new(1).unwrap(),
            Time::from_timestamp(1, 0),
            Time::from_timestamp(0, 1),
        );
    }

    #[test]
    fn starts_open_window() {
        let row_t0 = Time::from_timestamp_nanos(23526);
        let row_t1 = row_t0 + Duration::from_secs(1);
        let row_t2 = row_t1 + Duration::from_millis(3);
        let row_t3 = row_t2 + Duration::from_millis(3);

        let write_t0 = Time::from_timestamp_nanos(39832985493);
        let write_t1 = write_t0 + Duration::from_secs(2);
        let write_t2 = write_t1 + Duration::from_secs(2);
        let write_t3 = write_t2 + Duration::from_secs(2);

        let (mut w, time) = make_windows(Duration::from_secs(60), write_t0);

        // Write timestamps are purposefully out of order
        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(1).unwrap(),
            row_t0,
            row_t0,
        );
        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            NonZeroUsize::new(2).unwrap(),
            row_t1,
            row_t1,
        );
        time.set(write_t3);
        w.add_range(
            Some(&Sequence { id: 1, number: 10 }),
            NonZeroUsize::new(1).unwrap(),
            row_t2,
            row_t3,
        );
        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 2, number: 23 }),
            NonZeroUsize::new(10).unwrap(),
            row_t2,
            row_t3,
        );

        assert!(w.closed.is_empty());
        assert!(w.persistable.is_none());
        let open = w.open.unwrap();

        assert_eq!(open.time_of_last_write, write_t3);
        assert_eq!(open.min_time, row_t0);
        assert_eq!(open.max_time, row_t3);
        assert_eq!(open.row_count.get(), 14);
        assert_eq!(
            open.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence::new(2, 10)
        );
        assert_eq!(
            open.sequencer_numbers.get(&2).unwrap(),
            &MinMaxSequence::new(23, 23)
        );
    }

    #[test]
    fn closes_open_window() {
        let created_at = Time::from_timestamp_nanos(405693840963);
        let after_close_threshold = created_at + DEFAULT_CLOSED_WINDOW;

        let row_t0 = Time::from_timestamp_nanos(39049493);
        let row_t1 = row_t0 + Duration::from_secs(3);
        let row_t2 = row_t1 + Duration::from_millis(65);

        let (mut w, time) = make_windows(Duration::from_secs(60), created_at);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(1).unwrap(),
            row_t0,
            row_t1,
        );
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(1).unwrap(),
            row_t0,
            row_t1,
        );

        time.set(after_close_threshold);

        w.add_range(
            Some(&Sequence { id: 1, number: 6 }),
            NonZeroUsize::new(2).unwrap(),
            row_t1,
            row_t2,
        );

        assert!(w.persistable.is_none());

        assert_eq!(w.closed.len(), 1);
        let closed = w.closed.get(0).unwrap();
        assert_eq!(
            closed.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence::new(2, 3)
        );
        assert_eq!(closed.row_count.get(), 2);
        assert_eq!(closed.min_time, row_t0);
        assert_eq!(closed.max_time, row_t1);

        let open = w.open.unwrap();
        assert_eq!(open.row_count.get(), 2);
        assert_eq!(open.min_time, row_t1);
        assert_eq!(open.max_time, row_t2);
        assert_eq!(
            open.sequencer_numbers.get(&1).unwrap(),
            &MinMaxSequence::new(6, 6)
        )
    }

    #[test]
    fn moves_to_persistable() {
        let write_t0 = Time::from_timestamp_nanos(23459823490);
        let write_t1 = write_t0 + DEFAULT_CLOSED_WINDOW;
        let write_t2 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t3 = write_t2 + DEFAULT_CLOSED_WINDOW * 3;
        let write_t4 = write_t3 + DEFAULT_CLOSED_WINDOW * 100;

        let row_t0 = Time::from_timestamp_nanos(346363);
        let row_t1 = row_t0 + Duration::from_secs(4);
        let row_t2 = row_t1 + Duration::from_millis(393);
        let row_t3 = row_t2 + Duration::from_millis(493);
        let row_t4 = row_t3 + Duration::from_millis(5956);
        let row_t5 = row_t4 + Duration::from_millis(6997);

        let (mut w, time) = make_windows(Duration::from_secs(120), write_t0);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            row_t0,
            row_t1,
        );

        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(3).unwrap(),
            row_t1,
            row_t2,
        );

        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            NonZeroUsize::new(4).unwrap(),
            row_t2,
            row_t3,
        );

        assert!(w.persistable.is_none());
        // confirm the two on closed and third on open
        let c = w.closed.get(0).cloned().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, row_t0);
        assert_eq!(c.max_time, row_t1);

        let c = w.closed.get(1).cloned().unwrap();
        assert_eq!(c.time_of_first_write, write_t1);
        assert_eq!(c.row_count.get(), 3);
        assert_eq!(c.min_time, row_t1);
        assert_eq!(c.max_time, row_t2);

        let c = w.open.clone().unwrap();
        assert_eq!(c.time_of_first_write, write_t2);
        assert_eq!(c.row_count.get(), 4);
        assert_eq!(c.min_time, row_t2);
        assert_eq!(c.max_time, row_t3);

        time.set(write_t3);
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            NonZeroUsize::new(1).unwrap(),
            row_t4,
            row_t4,
        );

        // confirm persistable has first and second
        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 5);
        assert_eq!(c.min_time, row_t0);
        assert_eq!(c.max_time, row_t2);

        // and the third window moved to closed
        let c = w.closed.get(0).cloned().unwrap();
        assert_eq!(c.time_of_first_write, write_t2);
        assert_eq!(c.row_count.get(), 4);
        assert_eq!(c.min_time, row_t2);
        assert_eq!(c.max_time, row_t3);

        time.set(write_t4);
        w.add_range(
            Some(&Sequence { id: 1, number: 9 }),
            NonZeroUsize::new(2).unwrap(),
            row_t5,
            row_t5,
        );

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 10);
        assert_eq!(c.min_time, row_t0);
        assert_eq!(c.max_time, row_t4);
    }

    #[test]
    fn flush_persistable_keeps_open_and_closed() {
        // these instants represent when the server received the data. Here we have a window that
        // should be in the persistable group, a closed window, and an open window that is closed on flush.
        let write_t0 = Time::from_timestamp_nanos(546859);
        let write_t1 = write_t0 + DEFAULT_CLOSED_WINDOW * 2;
        let write_t2 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t3 = write_t2 + DEFAULT_CLOSED_WINDOW;

        // these times represent the value of the time column for the rows of data. Here we have
        // non-overlapping windows.
        let start_time = Time::from_timestamp_nanos(34693946939);
        let first_end = start_time + Duration::from_secs(1);
        let second_start = first_end + Duration::from_secs(1);
        let second_end = second_start + Duration::from_secs(1);
        let third_start = second_end + Duration::from_secs(1);
        let third_end = third_start + Duration::from_secs(1);

        let (mut w, time) = make_windows(Duration::from_secs(120), write_t0);

        time.set(write_t0);
        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            start_time,
            first_end,
        );

        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(3).unwrap(),
            second_start,
            second_end,
        );

        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            NonZeroUsize::new(2).unwrap(),
            third_start,
            third_end,
        );

        time.set(write_t3);
        w.rotate();

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        let handle = w.flush_handle().unwrap();
        w.flush(handle);

        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        let c = &w.closed[0];
        assert_eq!(c.row_count.get(), 3);
        assert_eq!(c.min_time, second_start);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.time_of_first_write, write_t1);

        let c = &w.closed[1];
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, third_start);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.time_of_first_write, write_t2);
    }

    #[test]
    fn flush_persistable_overlaps_closed() {
        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let write_t0 = Time::from_timestamp_nanos(3949);
        let write_t1 = write_t0 + DEFAULT_CLOSED_WINDOW * 2;
        let write_t2 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t3 = write_t2 + DEFAULT_CLOSED_WINDOW;

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the oldest closed window.
        let start_time = Time::from_timestamp_nanos(97945794);
        let second_start = start_time + Duration::from_secs(1);
        let first_end = second_start + Duration::from_secs(1);
        let second_end = first_end + Duration::from_secs(1);
        let third_start = first_end + Duration::from_secs(1);
        let third_end = third_start + Duration::from_secs(1);

        let (mut w, time) = make_windows(Duration::from_secs(120), write_t0);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            start_time,
            first_end,
        );

        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(3).unwrap(),
            second_start,
            second_end,
        );
        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            NonZeroUsize::new(2).unwrap(),
            third_start,
            third_end,
        );

        time.set(write_t3);
        w.rotate();

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());
        let flush = w.flush_handle().unwrap();

        assert_eq!(flush.timestamp(), first_end);
        let truncated_time = flush.timestamp() + Duration::from_nanos(1);

        w.flush(flush);
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // the first closed window should have a min time truncated by the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count.get(), 3);
        assert_eq!(c.min_time, truncated_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.time_of_first_write, write_t1);

        let c = &w.closed[1];
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, third_start);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.time_of_first_write, write_t2);
    }

    #[test]
    fn flush_persistable_overlaps_open() {
        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let write_t0 = Time::from_timestamp_nanos(346934603);
        let write_t1 = write_t0 + DEFAULT_CLOSED_WINDOW * 3;
        let write_t2 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t3 = write_t2 + Duration::from_secs(1);

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the newest open window (but not the closed one).
        let start_time = Time::from_timestamp_nanos(346934603);
        let third_start = start_time + Duration::from_secs(1);
        let first_end = third_start + Duration::from_secs(1);
        let second_end = first_end + Duration::from_secs(1);
        let third_end = second_end + Duration::from_secs(1);

        let (mut w, time) = make_windows(Duration::from_secs(120), write_t0);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            start_time,
            first_end,
        );
        time.set(write_t1);

        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(3).unwrap(),
            first_end,
            second_end,
        );
        time.set(write_t2);

        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            NonZeroUsize::new(2).unwrap(),
            third_start,
            third_end,
        );
        time.set(write_t3);

        w.rotate();

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        let flush = w.flush_handle().unwrap();
        assert_eq!(flush.timestamp(), first_end);
        assert!(w.open.is_none());
        let flushed_time = flush.timestamp() + Duration::from_nanos(1);

        w.flush(flush);
        assert!(w.persistable.is_none());

        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        assert_eq!(w.closed.len(), 2);

        // the closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count.get(), 3);
        assert_eq!(c.min_time, flushed_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.time_of_first_write, write_t1);

        // the open window should have been closed as part of creating the flush
        // handle and then truncated by the flush timestamp
        let c = &w.closed[1];
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, flushed_time);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.time_of_first_write, write_t2);
    }

    #[test]
    fn flush_persistable_overlaps_open_and_closed() {
        // these instants represent when data is received by the server. Here we have a persistable
        // window followed by two closed windows.
        let write_t0 = Time::from_timestamp_nanos(347094709);
        let write_t1 = write_t0 + DEFAULT_CLOSED_WINDOW * 3;
        let write_t2 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t3 = write_t2 + Duration::from_secs(1);

        // the times of the rows of data. this will create overlapping windows where persistable
        // overlaps with the closed window and the open one.
        let start_time = Time::from_timestamp_nanos(435760947094);
        let second_start = start_time + Duration::from_secs(1);
        let third_start = second_start + Duration::from_secs(1);
        let first_end = third_start + Duration::from_secs(1);
        let second_end = first_end + Duration::from_secs(1);
        let third_end = second_end + Duration::from_secs(1);

        let (mut w, time) = make_windows(Duration::from_secs(120), write_t0);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            start_time,
            first_end,
        );

        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(3).unwrap(),
            second_start,
            second_end,
        );

        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            NonZeroUsize::new(2).unwrap(),
            third_start,
            third_end,
        );

        let c = w.persistable.as_ref().unwrap();
        assert_eq!(c.time_of_first_write, write_t0);
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, start_time);
        assert_eq!(c.max_time, first_end);

        let mins = w.persistable.as_ref().unwrap().sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        // this should rotate the first window into persistable
        // after flush we should see no more persistable window and the closed windows
        // should have min timestamps equal to the previous flush end.
        time.set(write_t3);
        let flush = w.flush_handle().unwrap();
        assert_eq!(flush.timestamp(), first_end);
        assert!(w.open.is_none());
        let flushed_time = flush.timestamp() + Duration::from_nanos(1);
        w.flush(flush);
        assert!(w.persistable.is_none());
        let mins = w.closed[0].sequencer_numbers.clone();
        assert_eq!(mins, w.minimum_unpersisted_sequence().unwrap());

        assert_eq!(w.closed.len(), 2);

        // the closed window should have a min time equal to the flush
        let c = &w.closed[0];
        assert_eq!(c.row_count.get(), 3);
        assert_eq!(c.min_time, flushed_time);
        assert_eq!(c.max_time, second_end);
        assert_eq!(c.time_of_first_write, write_t1);

        // the open window should have been closed as part of creating the flush
        // handle and then truncated by the flush timestamp
        let c = &w.closed[1];
        assert_eq!(c.row_count.get(), 2);
        assert_eq!(c.min_time, flushed_time);
        assert_eq!(c.max_time, third_end);
        assert_eq!(c.time_of_first_write, write_t2);
    }

    #[test]
    fn test_flush_guard() {
        let late_arrival_period = Duration::from_secs(120);

        // Space writes so each goes to a separate window
        let write_t0 = Time::from_timestamp_nanos(565);
        let write_t1 = write_t0 + late_arrival_period;
        let write_t2 = write_t1 + late_arrival_period * 2;

        let row_t0 = Time::from_timestamp_nanos(340596340);
        let row_t1 = row_t0 + Duration::from_secs(2);
        let row_t2 = row_t1 + Duration::from_secs(2);

        let (mut w, time) = make_windows(late_arrival_period, write_t0);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            row_t0,
            row_t1,
        );

        time.set(write_t1);
        w.rotate();
        assert!(w.persistable.is_some());
        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 2);
        assert_eq!(w.persistable.as_ref().unwrap().max_time, row_t1);

        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            NonZeroUsize::new(5).unwrap(),
            row_t0,
            row_t2,
        );

        // Should rotate into closed
        time.set(write_t1 + DEFAULT_CLOSED_WINDOW);
        w.rotate();
        assert_eq!(w.closed.len(), 1);

        let guard = w.flush_handle().unwrap();

        // Should only allow one at once
        assert!(w.flush_handle().is_none());

        time.set(write_t1 + late_arrival_period);

        // This should not rotate into persistable as active flush guard
        w.rotate();
        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 2);

        let flush_t = guard.timestamp();
        assert_eq!(flush_t, row_t1);

        // Min time should have been truncated by persist operation to be
        // 1 nanosecond more than was persisted
        let truncated_time = flush_t + Duration::from_nanos(1);

        // The flush checkpoint should not include the writes being persisted
        let flush_checkpoint = guard.checkpoint();
        assert_eq!(
            flush_checkpoint.sequencer_numbers(1).unwrap(),
            OptionalMinMaxSequence::new(Some(4), 4)
        );
        assert_eq!(flush_checkpoint.flush_timestamp(), flush_t);

        // The sequencer numbers on the partition should include everything
        let sequencer_numbers = w.sequencer_numbers();
        assert_eq!(
            sequencer_numbers.get(&1).unwrap(),
            &OptionalMinMaxSequence::new(Some(2), 4)
        );

        // Flush persistable window
        w.flush(guard);
        assert!(w.persistable.is_none());

        // As there were no writes between creating the flush handle and the flush
        // the new partition sequencer numbers should match the persisted one
        let sequencer_numbers = w.sequencer_numbers();
        assert_eq!(
            &flush_checkpoint.sequencer_numbers(1).unwrap(),
            sequencer_numbers.get(&1).unwrap()
        );

        // This should rotate into persistable
        w.rotate();
        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 5);
        assert_eq!(w.persistable.as_ref().unwrap().min_time, truncated_time);

        let guard = w.flush_handle().unwrap();

        // that checkpoint has an optional minimum
        let flush_checkpoint = guard.checkpoint();
        assert_eq!(
            flush_checkpoint.sequencer_numbers(1).unwrap(),
            OptionalMinMaxSequence::new(None, 4)
        );

        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 9 }),
            NonZeroUsize::new(9).unwrap(),
            row_t0,
            row_t0 + Duration::from_secs(2),
        );

        // Should rotate into closed
        time.set(write_t2 + DEFAULT_CLOSED_WINDOW);
        w.rotate();
        assert_eq!(w.closed.len(), 1);

        // This should not rotate into persistable as active flush guard
        time.set(write_t2 + late_arrival_period);
        w.rotate();
        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 5);

        std::mem::drop(guard);
        // This should rotate into persistable
        w.rotate();
        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 5 + 9);
        assert_eq!(w.persistable.as_ref().unwrap().min_time, row_t0);
    }

    #[test]
    fn test_flush_guard_multiple_closed() {
        let late_arrival_period = DEFAULT_CLOSED_WINDOW * 3;

        // Space writes so each goes to a separate window
        let write_t0 = Time::from_timestamp_nanos(340634);
        let write_t1 = write_t0 + DEFAULT_CLOSED_WINDOW;
        let write_t2 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t3 = write_t2 + DEFAULT_CLOSED_WINDOW;
        let write_t4 = write_t3 + DEFAULT_CLOSED_WINDOW;

        let row_t0 = Time::from_timestamp_nanos(70780);
        let row_t1 = row_t0 + Duration::from_secs(2);
        let row_t2 = row_t1 + Duration::from_secs(2);

        let (mut w, time) = make_windows(late_arrival_period, write_t0);

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            row_t0,
            row_t1,
        );

        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 6 }),
            NonZeroUsize::new(5).unwrap(),
            row_t0,
            row_t2,
        );

        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 9 }),
            NonZeroUsize::new(9).unwrap(),
            row_t0,
            row_t1,
        );

        time.set(write_t3);
        w.add_range(
            Some(&Sequence { id: 1, number: 10 }),
            NonZeroUsize::new(17).unwrap(),
            row_t0,
            row_t1,
        );

        assert_eq!(w.closed.len(), 2);
        assert_eq!(w.closed[0].row_count.get(), 5);
        assert_eq!(w.closed[1].row_count.get(), 9);
        assert_eq!(w.open.as_ref().unwrap().row_count.get(), 17);

        let flush = w.flush_handle().unwrap();
        let flush_t = flush.timestamp();

        assert!(w.open.is_none());
        assert_eq!(flush.closed_count, 3);
        assert_eq!(flush_t, row_t1);
        let truncated_time = flush_t + Duration::from_nanos(1);

        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 2);

        time.set(write_t4);
        w.add_range(
            Some(&Sequence { id: 1, number: 14 }),
            NonZeroUsize::new(11).unwrap(),
            row_t0,
            row_t1,
        );

        time.set(write_t4 + DEFAULT_CLOSED_WINDOW);
        w.rotate();

        // Despite time passing persistable window shouldn't have changed due to flush guard
        assert_eq!(w.persistable.as_ref().unwrap().row_count.get(), 2);
        assert_eq!(w.closed.len(), 4);

        // The flush checkpoint should not include the latest write nor those being persisted
        let checkpoint = flush.checkpoint();
        assert_eq!(
            checkpoint.sequencer_numbers(1).unwrap(),
            OptionalMinMaxSequence::new(Some(6), 10)
        );
        assert_eq!(checkpoint.flush_timestamp(), flush_t);

        // The sequencer numbers of partition should include everything
        let sequencer_numbers = w.sequencer_numbers();
        assert_eq!(
            sequencer_numbers.get(&1).unwrap(),
            &OptionalMinMaxSequence::new(Some(2), 14)
        );

        w.flush(flush);

        // The sequencer numbers after the flush should include the new write
        let sequencer_numbers = w.sequencer_numbers();
        assert_eq!(
            sequencer_numbers.get(&1).unwrap(),
            &OptionalMinMaxSequence::new(Some(6), 14)
        );

        // Windows from writes 2 and 3 have been completely persisted by the flush

        assert!(w.persistable.is_none());
        assert_eq!(w.closed.len(), 2);

        assert_eq!(w.closed[0].time_of_first_write, write_t1);
        assert_eq!(w.closed[0].time_of_last_write, write_t1);
        assert_eq!(w.closed[0].min_time, truncated_time);
        assert_eq!(w.closed[0].max_time, row_t2);
        assert_eq!(w.closed[0].row_count.get(), 5);

        // Window created after flush handle - should be left alone
        assert_eq!(w.closed[1].time_of_first_write, write_t4);
        assert_eq!(w.closed[1].time_of_last_write, write_t4);
        assert_eq!(w.closed[1].min_time, row_t0);
        assert_eq!(w.closed[1].max_time, row_t1);
        assert_eq!(w.closed[1].row_count.get(), 11);
    }

    #[test]
    fn test_summaries() {
        let late_arrival_period = Duration::from_secs(100);
        let write_t0 = Time::from_timestamp_nanos(3963);
        let write_t1 = write_t0 + Duration::from_millis(1);
        let write_t2 = write_t1 + Duration::from_millis(29);
        let write_t3 = write_t2 + Duration::from_millis(20);
        let write_t4 = write_t1 + DEFAULT_CLOSED_WINDOW;
        let write_t5 = write_t0 + DEFAULT_CLOSED_WINDOW * 3;
        let write_t6 = write_t4 + late_arrival_period;

        let (mut w, time) = make_windows(late_arrival_period, write_t0);

        // Window 1
        time.set(write_t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 1 }),
            NonZeroUsize::new(11).unwrap(),
            Time::from_timestamp_nanos(10),
            Time::from_timestamp_nanos(11),
        );

        time.set(write_t2);
        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(4).unwrap(),
            Time::from_timestamp_nanos(10),
            Time::from_timestamp_nanos(340),
        );

        time.set(write_t3);
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(6).unwrap(),
            Time::from_timestamp_nanos(1),
            Time::from_timestamp_nanos(5),
        );

        // More than DEFAULT_CLOSED_WINDOW_PERIOD after start of Window 1 => Window 2
        time.set(write_t4);
        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            NonZeroUsize::new(3).unwrap(),
            Time::from_timestamp_nanos(89),
            Time::from_timestamp_nanos(90),
        );

        // More than DEFAULT_CLOSED_WINDOW_PERIOD after start of Window 2 => Window 3
        time.set(write_t5);
        w.add_range(
            Some(&Sequence { id: 1, number: 5 }),
            NonZeroUsize::new(8).unwrap(),
            Time::from_timestamp_nanos(3),
            Time::from_timestamp_nanos(4),
        );

        let summaries: Vec<_> = w.summaries().collect();

        assert_eq!(summaries.len(), 3);
        assert_eq!(
            summaries,
            vec![
                WriteSummary {
                    time_of_first_write: write_t1,
                    time_of_last_write: write_t3,
                    min_timestamp: Time::from_timestamp_nanos(1),
                    max_timestamp: Time::from_timestamp_nanos(340),
                    row_count: 21
                },
                WriteSummary {
                    time_of_first_write: write_t4,
                    time_of_last_write: write_t4,
                    min_timestamp: Time::from_timestamp_nanos(89),
                    max_timestamp: Time::from_timestamp_nanos(90),
                    row_count: 3
                },
                WriteSummary {
                    time_of_first_write: write_t5,
                    time_of_last_write: write_t5,
                    min_timestamp: Time::from_timestamp_nanos(3),
                    max_timestamp: Time::from_timestamp_nanos(4),
                    row_count: 8
                },
            ]
        );

        // Rotate first and second windows into persistable
        time.set(write_t6);
        w.rotate();

        let summaries: Vec<_> = w.summaries().collect();

        assert_eq!(summaries.len(), 2);
        assert_eq!(
            summaries,
            vec![
                WriteSummary {
                    time_of_first_write: write_t1,
                    time_of_last_write: write_t4,
                    min_timestamp: Time::from_timestamp_nanos(1),
                    max_timestamp: Time::from_timestamp_nanos(340),
                    row_count: 24
                },
                WriteSummary {
                    time_of_first_write: write_t5,
                    time_of_last_write: write_t5,
                    min_timestamp: Time::from_timestamp_nanos(3),
                    max_timestamp: Time::from_timestamp_nanos(4),
                    row_count: 8
                },
            ]
        );
    }

    #[test]
    fn test_regression_2206() {
        let late_arrival_period = DEFAULT_CLOSED_WINDOW * 10;

        let t0 = Time::from_timestamp_nanos(47069490749);
        let t1 = t0 + late_arrival_period;

        let (mut w, time) = make_windows(late_arrival_period, t0);

        // window 1: to be persisted
        let min_time = Time::from_timestamp_nanos(10);
        let max_time = Time::from_timestamp_nanos(11);
        w.add_range(
            Some(&Sequence { id: 1, number: 1 }),
            NonZeroUsize::new(1).unwrap(),
            min_time,
            max_time,
        );

        // window 2: closed but overlaps with the persistence range
        time.set(t1);
        w.add_range(
            Some(&Sequence { id: 1, number: 4 }),
            NonZeroUsize::new(1).unwrap(),
            min_time,
            max_time,
        );

        // persist
        let handle = w.flush_handle().unwrap();
        let ckpt = handle.checkpoint();
        w.flush(handle);

        // speculated checkpoint should be correct
        let ckpt_sequencer_numbers: BTreeMap<_, _> = ckpt.sequencer_numbers_iter().collect();
        assert_eq!(w.sequencer_numbers(), ckpt_sequencer_numbers);
    }

    #[test]
    fn test_mark_seen_and_persisted() {
        let late_arrival_period = Duration::from_secs(100);
        let t0 = Time::from_timestamp_nanos(47069490749);

        let (mut w, _) = make_windows(late_arrival_period, t0);

        let mut sequencer_numbers1 = BTreeMap::new();
        sequencer_numbers1.insert(1, OptionalMinMaxSequence::new(Some(1), 2));
        let ckpt1 = PartitionCheckpoint::new(
            Arc::from("foo"),
            Arc::from("bar"),
            sequencer_numbers1,
            Time::from_timestamp_nanos(260936036),
        );
        w.mark_seen_and_persisted(&ckpt1);

        let mut sequencer_numbers2 = BTreeMap::new();
        sequencer_numbers2.insert(1, OptionalMinMaxSequence::new(Some(0), 1));
        sequencer_numbers2.insert(2, OptionalMinMaxSequence::new(None, 3));
        let ckpt2 = PartitionCheckpoint::new(
            Arc::from("foo"),
            Arc::from("bar"),
            sequencer_numbers2,
            Time::from_timestamp_nanos(345345),
        );
        w.mark_seen_and_persisted(&ckpt2);

        let actual = w.sequencer_numbers();
        let mut expected = BTreeMap::new();
        expected.insert(1, OptionalMinMaxSequence::new(None, 2));
        expected.insert(2, OptionalMinMaxSequence::new(None, 3));
        assert_eq!(actual, expected);
    }

    #[test]
    fn flush_min_max_timestamp() {
        let (mut w, time) = make_windows(Duration::from_secs(30), Time::from_timestamp_nanos(0));

        w.add_range(
            Some(&Sequence { id: 1, number: 2 }),
            NonZeroUsize::new(2).unwrap(),
            Time::MIN,
            Time::MAX,
        );
        time.inc(Duration::from_secs(30));
        w.add_range(
            Some(&Sequence { id: 1, number: 3 }),
            NonZeroUsize::new(2).unwrap(),
            Time::MIN,
            Time::MAX,
        );

        time.inc(Duration::from_secs(3));
        let handle = w.flush_handle().unwrap();
        assert_eq!(handle.timestamp(), Time::MAX);
        let ckpt = handle.checkpoint();
        assert_eq!(ckpt.flush_timestamp(), Time::MAX);
        w.flush(handle);

        assert!(w.closed.is_empty());
        assert!(w.persistable.is_none());
    }
}

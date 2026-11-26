use crate::types::{CommandPriority, CommandType, QueuedCommand};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, info, warn};
use uuid::Uuid;

const BAZAAR_RECOMMENDATION_MAX_AGE_MS: u64 = 60_000; // 60 seconds
#[allow(dead_code)]
const COMMAND_TIMEOUT_MS: u64 = 30_000; // 30 seconds
/// Maximum number of queued commands (excluding the currently executing one).
/// When exceeded, low-priority commands are trimmed — but sell offers and AH
/// listings are always kept because they represent revenue opportunities.
const MAX_QUEUE_SIZE: usize = 5;

/// Expected profit of an AH buy (`target - starting_bid`), or `None` when the
/// command is not a `PurchaseAuction`.
fn buy_profit(command_type: &CommandType) -> Option<u64> {
    match command_type {
        CommandType::PurchaseAuction { flip } => {
            Some(flip.target.saturating_sub(flip.starting_bid))
        }
        _ => None,
    }
}

/// Why a queued AH buy should be skipped instead of attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuySkipReason {
    /// The flip has been buyable for longer than the configured max age, so
    /// it has almost certainly been sniped already.
    Stale { age_ms: u64 },
    /// The known purse is below the starting bid.
    Unaffordable { purse: u64, price: u64 },
}

/// Decide whether a buy that is about to run is still worth attempting.
///
/// Age is measured from the later of "when it was queued" and "when COFL says
/// it becomes buyable" (`purchase_at_ms`, the grace-period end), so a flip
/// that is deliberately sent early for bed timing is not counted as stale
/// while it waits for its grace period. `max_age_ms == 0` disables the age
/// check; an unknown purse (`None`) disables the affordability check.
pub fn buy_skip_reason(
    cmd: &QueuedCommand,
    now: Instant,
    now_unix_ms: i64,
    max_age_ms: u64,
    purse: Option<u64>,
) -> Option<BuySkipReason> {
    let CommandType::PurchaseAuction { flip } = &cmd.command_type else {
        return None;
    };
    if let Some(purse) = purse {
        if flip.starting_bid > purse {
            return Some(BuySkipReason::Unaffordable {
                purse,
                price: flip.starting_bid,
            });
        }
    }
    if max_age_ms > 0 {
        let since_queued = now.saturating_duration_since(cmd.queued_at).as_millis() as u64;
        let age_ms = match flip.purchase_at_ms {
            // Not buyable yet, or buyable for less time than it has been queued.
            Some(at) => since_queued.min(now_unix_ms.saturating_sub(at).max(0) as u64),
            None => since_queued,
        };
        if age_ms > max_age_ms {
            return Some(BuySkipReason::Stale { age_ms });
        }
    }
    None
}

#[derive(Clone)]
pub struct CommandQueue {
    queue: Arc<RwLock<VecDeque<QueuedCommand>>>,
    current_command: Arc<RwLock<Option<QueuedCommand>>>,
    /// Notified whenever a new command is enqueued so the processor loop can
    /// wake up immediately instead of polling every 50 ms.
    notify: Arc<Notify>,
}

impl CommandQueue {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(RwLock::new(VecDeque::new())),
            current_command: Arc::new(RwLock::new(None)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Add a command to the queue.  If the queue exceeds `MAX_QUEUE_SIZE`,
    /// low-priority commands are trimmed — but sell offers, AH listings, and
    /// ManageOrders are never dropped because they represent revenue or are
    /// essential housekeeping.
    pub fn enqueue(
        &self,
        command_type: CommandType,
        priority: CommandPriority,
        interruptible: bool,
    ) -> Uuid {
        // `ClaimPurchasedItem` is an argument-free bulk sweep: it opens /ah, walks to
        // "Your Bids" and hits "Claim All", which drains EVERY pending purchase in one
        // go.  A second queued copy therefore has nothing left to claim and only
        // repeats the /ah -> Your Bids window churn.  On 2026-07-28 a redundant copy
        // fired 13 ms after the first completed, opened a window the server closed
        // 65 ms later, and clicked into it: Limbo, then a 30-day Watchdog ban
        // (#01E34397).  Collapse duplicates onto the pending one instead of queueing.
        // Returning the existing id keeps `await_queued_command` waiting on the real
        // command rather than an id that was never enqueued.
        if matches!(command_type, CommandType::ClaimPurchasedItem) {
            let existing = self
                .queue
                .read()
                .iter()
                .find(|c| matches!(c.command_type, CommandType::ClaimPurchasedItem))
                .map(|c| c.id);
            if let Some(existing_id) = existing {
                debug!(
                    "[Queue] Collapsed duplicate ClaimPurchasedItem onto pending {}",
                    existing_id
                );
                return existing_id;
            }
        }

        let id = Uuid::new_v4();
        let cmd = QueuedCommand {
            id,
            priority,
            command_type: command_type.clone(),
            queued_at: Instant::now(),
            interruptible,
        };

        let mut queue = self.queue.write();

        // Find insertion point based on priority. AH buys go ahead of every
        // other queued command, Critical ones included (manual panel actions,
        // listings, claims): a snipe is only worth anything in the first few
        // hundred ms, while those can run a moment later. Among themselves,
        // buys are ordered by expected profit (highest first), arrival order
        // breaking ties: when several are waiting behind a running buy, the
        // next attempt should go to the one worth the most.
        let pos = if let Some(profit) = buy_profit(&command_type) {
            queue
                .iter()
                .position(|c| buy_profit(&c.command_type).is_none_or(|p| p < profit))
                .unwrap_or(queue.len())
        } else {
            queue
                .iter()
                .position(|c| c.priority > priority)
                .unwrap_or(queue.len())
        };

        queue.insert(pos, cmd);

        // Enforce maximum queue size by removing low-priority droppable
        // commands from the back.  Important commands (sell offers, AH
        // listings, ManageOrders) are never dropped.
        while queue.len() > MAX_QUEUE_SIZE {
            // Find the last droppable (non-essential) command
            if let Some(drop_idx) = queue.iter().rposition(|c| {
                !matches!(
                    c.command_type,
                    CommandType::BazaarSellOrder { .. }
                        | CommandType::SellToAuction { .. }
                        | CommandType::ManageOrders { .. }
                        | CommandType::PurchaseAuction { .. }
                )
            }) {
                let dropped = queue.remove(drop_idx).unwrap();
                info!(
                    "[Queue] Trimmed excess command {:?} ({:?}) — queue over {} limit",
                    dropped.command_type.display_name(),
                    dropped.priority,
                    MAX_QUEUE_SIZE
                );
            } else {
                // All commands are essential — allow the queue to exceed the limit.
                break;
            }
        }

        drop(queue);

        // Wake the command processor loop so it picks up the new command immediately.
        self.notify.notify_one();

        debug!(
            "Enqueued command {:?} with priority {:?} at position {}",
            id, priority, pos
        );
        id
    }

    /// Get the next command without removing it
    pub fn peek(&self) -> Option<QueuedCommand> {
        // First check if there's a current command
        if let Some(current) = self.current_command.read().as_ref() {
            return Some(current.clone());
        }

        // Remove stale bazaar commands (older than 60 seconds)
        self.remove_stale_commands();

        self.queue.read().front().cloned()
    }

    /// Mark the current command as started
    pub fn start_current(&self) -> Option<QueuedCommand> {
        let mut queue = self.queue.write();
        if let Some(cmd) = queue.pop_front() {
            *self.current_command.write() = Some(cmd.clone());
            info!(
                "Starting command {:?} (priority: {:?})",
                cmd.id, cmd.priority
            );
            Some(cmd)
        } else {
            None
        }
    }

    /// Complete the current command
    pub fn complete_current(&self) {
        if let Some(cmd) = self.current_command.write().take() {
            info!("Completed command {:?}", cmd.id);
        }
    }

    /// Check if current command can be interrupted
    pub fn can_interrupt_current(&self) -> bool {
        self.current_command
            .read()
            .as_ref()
            .map(|c| c.interruptible)
            .unwrap_or(true)
    }

    /// Interrupt current command
    pub fn interrupt_current(&self) {
        if let Some(cmd) = self.current_command.write().take() {
            warn!("Interrupted command {:?}", cmd.id);
        }
    }

    /// Peek at the next command waiting in the queue (ignoring the
    /// currently-executing command).  Used by the processor loop to detect
    /// higher-priority commands that should preempt the running one.
    pub fn peek_queued(&self) -> Option<QueuedCommand> {
        self.queue.read().front().cloned()
    }

    /// True when an AH buy is waiting in the queue. The processor uses this to
    /// cut the inter-command rest short so a buy never sits through the
    /// anti-spam delay of the previous command. Only buys count: other Critical
    /// commands (e.g. a panel-triggered listing) must still respect the rest,
    /// or back-to-back listings risk "Sending packets too fast" kicks.
    pub fn critical_queued(&self) -> bool {
        self.queue
            .read()
            .front()
            .is_some_and(|c| matches!(c.command_type, CommandType::PurchaseAuction { .. }))
    }

    /// Clear all bazaar orders from queue
    pub fn clear_bazaar_orders(&self) {
        let mut queue = self.queue.write();
        queue.retain(|cmd| {
            !matches!(
                cmd.command_type,
                CommandType::BazaarBuyOrder { .. } | CommandType::BazaarSellOrder { .. }
            )
        });
        info!("Cleared all bazaar orders from queue");
    }

    /// Clear all commands from queue (matching TypeScript clearQueue)
    pub fn clear(&self) {
        let mut queue = self.queue.write();
        queue.clear();
        info!("Cleared all commands from queue");
    }

    /// Remove stale commands (bazaar orders older than 60 seconds)
    fn remove_stale_commands(&self) {
        let mut queue = self.queue.write();
        let now = Instant::now();
        let max_age = Duration::from_millis(BAZAAR_RECOMMENDATION_MAX_AGE_MS);

        let original_len = queue.len();
        queue.retain(|cmd| {
            let age = now.duration_since(cmd.queued_at);

            // Only remove stale bazaar commands
            if matches!(
                cmd.command_type,
                CommandType::BazaarBuyOrder { .. } | CommandType::BazaarSellOrder { .. }
            ) && age > max_age
            {
                debug!("Removing stale command {:?} (age: {:?})", cmd.id, age);
                false
            } else {
                true
            }
        });

        if queue.len() < original_len {
            info!(
                "Removed {} stale command(s) from queue",
                original_len - queue.len()
            );
        }
    }

    /// Get queue size
    pub fn len(&self) -> usize {
        self.queue.read().len()
    }

    /// Returns true if a ManageOrders command is already queued or currently
    /// executing.  Used to avoid duplicate ManageOrders runs that produce
    /// duplicate Hypixel chat messages and wasted GUI interactions.
    pub fn has_manage_orders(&self) -> bool {
        if let Some(ref cur) = *self.current_command.read() {
            if matches!(cur.command_type, CommandType::ManageOrders { .. }) {
                return true;
            }
        }
        self.queue
            .read()
            .iter()
            .any(|c| matches!(c.command_type, CommandType::ManageOrders { .. }))
    }

    /// Returns true if a ClaimSoldItem command is queued or currently executing.
    /// Used to coalesce repeated `collectAuctions` signals into a single claim.
    pub fn has_claim_sold(&self) -> bool {
        if let Some(ref cur) = *self.current_command.read() {
            if matches!(cur.command_type, CommandType::ClaimSoldItem) {
                return true;
            }
        }
        self.queue
            .read()
            .iter()
            .any(|c| matches!(c.command_type, CommandType::ClaimSoldItem))
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.queue.read().is_empty() && self.current_command.read().is_none()
    }

    /// Returns true if a command with the given ID is still queued or currently executing.
    /// Used by the startup workflow to wait until a specific enqueued command has completed.
    pub fn contains_command_id(&self, id: &Uuid) -> bool {
        if let Some(ref cur) = *self.current_command.read() {
            if cur.id == *id {
                return true;
            }
        }
        self.queue.read().iter().any(|c| c.id == *id)
    }

    /// Returns a future that resolves when a new command is enqueued.
    /// Used by the command processor loop to sleep efficiently instead of
    /// polling every 50 ms.
    ///
    /// Callers should create and pin this future **before** checking the queue
    /// with `start_current()` to avoid missing notifications:
    /// ```ignore
    /// let notified = queue.notified();
    /// tokio::pin!(notified);
    /// if let Some(cmd) = queue.start_current() { /* process */ }
    /// else { notified.await; }
    /// ```
    pub fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    /// Return a snapshot of the queue for the web panel.
    /// Includes the currently executing command (if any) followed by all
    /// queued commands, each described by its display name, priority, and age.
    pub fn queue_snapshot(&self) -> Vec<QueueEntry> {
        let mut entries = Vec::new();
        if let Some(ref cur) = *self.current_command.read() {
            entries.push(QueueEntry {
                display_name: cur.command_type.display_name().to_string(),
                priority: format!("{:?}", cur.priority),
                age_ms: cur.queued_at.elapsed().as_millis() as u64,
                executing: true,
            });
        }
        for cmd in self.queue.read().iter() {
            entries.push(QueueEntry {
                display_name: cmd.command_type.display_name().to_string(),
                priority: format!("{:?}", cmd.priority),
                age_ms: cmd.queued_at.elapsed().as_millis() as u64,
                executing: false,
            });
        }
        entries
    }
}

/// A single entry in the queue snapshot, serializable for the web API.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueueEntry {
    pub display_name: String,
    pub priority: String,
    pub age_ms: u64,
    pub executing: bool,
}

impl Default for CommandQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_bazaar_buy() -> CommandType {
        CommandType::BazaarBuyOrder {
            item_name: "Coal".into(),
            item_tag: None,
            amount: 64,
            price_per_unit: 10.0,
        }
    }

    fn dummy_purchase_auction() -> CommandType {
        use crate::types::Flip;
        CommandType::PurchaseAuction {
            flip: Flip {
                item_name: "Diamond".into(),
                starting_bid: 800,
                target: 1000,
                finder: Some("SNIPER".into()),
                profit_perc: None,
                purchase_at_ms: None,
                uuid: Some("abc".into()),
                list_at: None,
            },
        }
    }

    #[test]
    fn critical_priority_comes_before_normal() {
        let q = CommandQueue::new();
        // Enqueue a normal-priority bazaar buy
        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        // Enqueue a critical-priority AH flip
        q.enqueue(dummy_purchase_auction(), CommandPriority::Critical, false);

        // Critical should be at front
        let front = q.peek().unwrap();
        assert_eq!(front.priority, CommandPriority::Critical);
    }

    #[test]
    fn peek_queued_ignores_current_command() {
        let q = CommandQueue::new();
        // Enqueue two commands
        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        q.enqueue(dummy_purchase_auction(), CommandPriority::Critical, false);

        // Start current (pops the Critical one since it's first)
        let started = q.start_current().unwrap();
        assert_eq!(started.priority, CommandPriority::Critical);

        // peek_queued should return the remaining Normal command, not the
        // currently-executing Critical one
        let next = q.peek_queued().unwrap();
        assert_eq!(next.priority, CommandPriority::Normal);
    }

    #[test]
    fn critical_queued_sees_only_waiting_buys() {
        let q = CommandQueue::new();
        assert!(!q.critical_queued());
        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        assert!(!q.critical_queued());
        q.enqueue(dummy_purchase_auction(), CommandPriority::Critical, false);
        assert!(q.critical_queued());
        // once the buy is running it is no longer "waiting"
        let _ = q.start_current();
        assert!(!q.critical_queued());
    }

    #[test]
    fn buy_jumps_ahead_of_other_critical_commands() {
        let q = CommandQueue::new();
        q.enqueue(dummy_manage_orders(), CommandPriority::Critical, false);
        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        q.enqueue(dummy_purchase_auction(), CommandPriority::Critical, false);
        let first = q.start_current().unwrap();
        assert!(matches!(
            first.command_type,
            CommandType::PurchaseAuction { .. }
        ));
    }

    fn buy_with(starting_bid: u64, target: u64, purchase_at_ms: Option<i64>) -> CommandType {
        use crate::types::Flip;
        CommandType::PurchaseAuction {
            flip: Flip {
                item_name: "Diamond".into(),
                starting_bid,
                target,
                finder: Some("SNIPER".into()),
                profit_perc: None,
                purchase_at_ms,
                uuid: Some("abc".into()),
                list_at: None,
            },
        }
    }

    fn queued(command_type: CommandType, queued_at: Instant) -> QueuedCommand {
        QueuedCommand {
            id: Uuid::new_v4(),
            priority: CommandPriority::Critical,
            command_type,
            queued_at,
            interruptible: false,
        }
    }

    #[test]
    fn buys_are_ordered_by_profit() {
        let q = CommandQueue::new();
        let small = q.enqueue(buy_with(900, 1_000, None), CommandPriority::Critical, false);
        let big = q.enqueue(
            buy_with(1_000, 5_000, None),
            CommandPriority::Critical,
            false,
        );
        let mid = q.enqueue(
            buy_with(1_000, 2_000, None),
            CommandPriority::Critical,
            false,
        );
        q.enqueue(dummy_manage_orders(), CommandPriority::Critical, false);
        for expected in [big, mid, small] {
            assert_eq!(q.start_current().unwrap().id, expected);
            q.complete_current();
        }
    }

    #[test]
    fn fresh_affordable_buy_is_kept() {
        let now = Instant::now();
        let cmd = queued(buy_with(800, 1_000, None), now);
        assert_eq!(buy_skip_reason(&cmd, now, 0, 5_000, Some(10_000)), None);
    }

    #[test]
    fn old_buy_is_stale() {
        let now = Instant::now();
        let cmd = queued(buy_with(800, 1_000, None), now - Duration::from_secs(10));
        assert!(matches!(
            buy_skip_reason(&cmd, now, 0, 5_000, None),
            Some(BuySkipReason::Stale { .. })
        ));
        // 0 disables the age check
        assert_eq!(buy_skip_reason(&cmd, now, 0, 0, None), None);
    }

    #[test]
    fn grace_period_buy_is_not_stale_before_it_opens() {
        let now = Instant::now();
        let now_ms = 1_800_000_000_000;
        // queued 10 s ago, but only buyable from 1 s from now
        let cmd = queued(
            buy_with(800, 1_000, Some(now_ms + 1_000)),
            now - Duration::from_secs(10),
        );
        assert_eq!(buy_skip_reason(&cmd, now, now_ms, 5_000, None), None);
    }

    #[test]
    fn unaffordable_buy_is_skipped() {
        let now = Instant::now();
        let cmd = queued(buy_with(800, 1_000, None), now);
        assert_eq!(
            buy_skip_reason(&cmd, now, 0, 5_000, Some(500)),
            Some(BuySkipReason::Unaffordable {
                purse: 500,
                price: 800
            })
        );
    }

    #[test]
    fn equal_profit_buys_keep_arrival_order() {
        let q = CommandQueue::new();
        let first = q.enqueue(dummy_purchase_auction(), CommandPriority::Critical, false);
        let second = q.enqueue(dummy_purchase_auction(), CommandPriority::Critical, false);
        assert_eq!(q.start_current().unwrap().id, first);
        q.complete_current();
        assert_eq!(q.start_current().unwrap().id, second);
    }

    #[test]
    fn other_critical_commands_do_not_cut_the_rest_short() {
        let q = CommandQueue::new();
        q.enqueue(dummy_manage_orders(), CommandPriority::Critical, false);
        assert!(!q.critical_queued());
    }

    #[test]
    fn peek_queued_returns_none_when_empty() {
        let q = CommandQueue::new();
        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        let _ = q.start_current(); // pop it
        assert!(q.peek_queued().is_none());
    }

    #[test]
    fn interrupt_clears_current_command() {
        let q = CommandQueue::new();
        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        let started = q.start_current().unwrap();
        assert!(started.interruptible);
        assert!(q.can_interrupt_current());

        q.interrupt_current();
        // After interruption, there is no current command
        assert!(q.is_empty());
    }

    fn dummy_manage_orders() -> CommandType {
        CommandType::ManageOrders {
            cancel_open: false,
            target_item: None,
        }
    }

    #[test]
    fn has_manage_orders_detects_queued() {
        let q = CommandQueue::new();
        assert!(!q.has_manage_orders());

        q.enqueue(dummy_bazaar_buy(), CommandPriority::Normal, true);
        assert!(!q.has_manage_orders());

        q.enqueue(dummy_manage_orders(), CommandPriority::Normal, false);
        assert!(q.has_manage_orders());
    }

    #[test]
    fn has_manage_orders_detects_running() {
        let q = CommandQueue::new();
        q.enqueue(dummy_manage_orders(), CommandPriority::High, false);
        assert!(q.has_manage_orders());

        // Start executing it — should still be detected as current
        let _ = q.start_current();
        assert!(q.has_manage_orders());

        // Complete it — should no longer be detected
        q.complete_current();
        assert!(!q.has_manage_orders());
    }
}

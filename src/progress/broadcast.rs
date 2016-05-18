//! Broadcasts progress information among workers.

use progress::Timestamp;
use progress::count_map::CountMap;
use timely_communication::Allocate;
use {Push, Pull};

/// A list of progress updates corresponding to `((child_scope, [in/out]_port, timestamp), delta)`
pub type ProgressVec<T> = Vec<((usize, usize, T), i64)>;
/// A progress update message consisting of source worker id, sequence number and lists of 
/// message and internal updates
pub type ProgressMsg<T> = (usize, usize, ProgressVec<T>, ProgressVec<T>);

/// Manages broadcasting of progress updates to and receiving updates from workers.
pub struct Progcaster<T:Timestamp> {
    pushers: Vec<Box<Push<ProgressMsg<T>>>>,
    puller: Box<Pull<ProgressMsg<T>>>,
    /// Source worker index
    source: usize,
    /// Sequence number counter
    counter: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this subgraph
    addr: Vec<usize>,
}

impl<T:Timestamp+Send> Progcaster<T> {
    /// Creates a new `Progcaster` using a channel from the supplied allocator.
    pub fn new<A: Allocate>(allocator: &mut A, path: &Vec<usize>) -> Progcaster<T> {
        let (pushers, puller) = allocator.allocate();
        let worker = allocator.index();
        let addr = path.clone();
        Progcaster { pushers: pushers, puller: puller, source: worker, counter: 0, addr: addr }
    }

    // TODO : puller.pull() forcibly decodes, whereas we would be just as happy to read data from
    // TODO : binary. Investigate fabric::Wrapper for this purpose.
    /// Sends and receives progress updates, broadcasting the contents of `messages` and `internal`,
    /// and updating each with updates from other workers.
    pub fn send_and_recv(
        &mut self,
        messages: &mut CountMap<(usize, usize, T)>,
        internal: &mut CountMap<(usize, usize, T)>)
    {

        // // we should not be sending zero deltas.
        // assert!(messages.iter().all(|x| x.1 != 0));
        // assert!(internal.iter().all(|x| x.1 != 0));
        if self.pushers.len() > 1 {  // if the length is one, just return the updates...
            if messages.len() > 0 || internal.len() > 0 {

                ::logging::log(&::logging::PROGRESS, ::logging::ProgressEvent {
                    is_send: true,
                    source: self.source,
                    seq_no: self.counter,
                    addr: self.addr.clone(),
                    // TODO: fill with additional data
                    messages: Vec::new(),
                    internal: Vec::new(),
                });

                for pusher in self.pushers.iter_mut() {
                    // TODO : Feels like an Arc might be not horrible here... less allocation,
                    // TODO : at least, but more "contention" in the deallocation.
                    pusher.push(&mut Some((self.source,
                                           self.counter,
                                           messages.clone().into_inner(),
                                           internal.clone().into_inner()
                                          ))
                                );
                }
                self.counter += 1;

                messages.clear();
                internal.clear();
            }

            // TODO : Could take ownership, and recycle / reuse for next broadcast ...
            while let Some((source, seq_no, ref recv_messages, ref recv_internal)) = *self.puller.pull() {

                ::logging::log(&::logging::PROGRESS, ::logging::ProgressEvent {
                    is_send: false,
                    source: source,
                    seq_no: seq_no,
                    addr: self.addr.clone(),
                    // TODO: fill with additional data
                    messages: Vec::new(),
                    internal: Vec::new(),
                });

                for &(ref update, delta) in recv_messages {
                    messages.update(update, delta);
                }
                for &(ref update, delta) in recv_internal {
                    internal.update(update, delta);
                }
            }
        }
    }
}

(function() {var implementors = {};
implementors['libc'] = [];implementors['timely_communication'] = ["impl&lt;T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Send.html' title='core::marker::Send'>Send</a> + 'static&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='timely_communication/initialize/struct.WorkerGuards.html' title='timely_communication::initialize::WorkerGuards'>WorkerGuards</a>&lt;T&gt;",];implementors['timely'] = ["impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='timely_communication/initialize/struct.WorkerGuards.html' title='timely_communication::initialize::WorkerGuards'>WorkerGuards</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Send.html' title='core::marker::Send'>Send</a> + 'static</span>","impl&lt;'a, T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='timely_communication/drain/struct.Drain.html' title='timely_communication::drain::Drain'>Drain</a>&lt;'a, T&gt;","impl&lt;T: <a class='trait' href='timely/progress/timestamp/trait.Timestamp.html' title='timely::progress::timestamp::Timestamp'>Timestamp</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Ord.html' title='core::cmp::Ord'>Ord</a>, D: <a class='trait' href='timely/trait.Data.html' title='timely::Data'>Data</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='timely/dataflow/operators/input/struct.Handle.html' title='timely::dataflow::operators::input::Handle'>Handle</a>&lt;T, D&gt;","impl&lt;T: <a class='trait' href='timely/progress/timestamp/trait.Timestamp.html' title='timely::progress::timestamp::Timestamp'>Timestamp</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='timely/dataflow/operators/struct.Capability.html' title='timely::dataflow::operators::Capability'>Capability</a>&lt;T&gt;","impl&lt;'a, T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/binary_heap/struct.Hole.html' title='collections::binary_heap::Hole'>Hole</a>&lt;'a, T&gt;","impl&lt;K, V&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/btree/map/struct.BTreeMap.html' title='collections::btree::map::BTreeMap'>BTreeMap</a>&lt;K, V&gt;","impl&lt;K, V&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/btree/map/struct.IntoIter.html' title='collections::btree::map::IntoIter'>IntoIter</a>&lt;K, V&gt;","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/linked_list/struct.LinkedList.html' title='collections::linked_list::LinkedList'>LinkedList</a>&lt;T&gt;","impl&lt;'a&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/string/struct.Drain.html' title='collections::string::Drain'>Drain</a>&lt;'a&gt;","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec/struct.Vec.html' title='collections::vec::Vec'>Vec</a>&lt;T&gt;","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec/struct.IntoIter.html' title='collections::vec::IntoIter'>IntoIter</a>&lt;T&gt;","impl&lt;'a, T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec/struct.Drain.html' title='collections::vec::Drain'>Drain</a>&lt;'a, T&gt;","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec_deque/struct.VecDeque.html' title='collections::vec_deque::VecDeque'>VecDeque</a>&lt;T&gt;","impl&lt;'a, T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec_deque/struct.Drain.html' title='collections::vec_deque::Drain'>Drain</a>&lt;'a, T&gt; <span class='where'>where T: 'a</span>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/alloc/boxed/struct.IntermediateBox.html' title='alloc::boxed::IntermediateBox'>IntermediateBox</a>&lt;T&gt; <span class='where'>where T: ?<a class='trait' href='https://doc.rust-lang.org/nightly/core/marker/trait.Sized.html' title='core::marker::Sized'>Sized</a></span>","impl&lt;T: <a class='trait' href='timely/trait.Data.html' title='timely::Data'>Data</a>, S: <a class='trait' href='https://doc.rust-lang.org/nightly/std/io/trait.Write.html' title='std::io::Write'>Write</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.Drop.html' title='core::ops::Drop'>Drop</a> for <a class='struct' href='timely/logging/struct.EventStreamLogger.html' title='timely::logging::EventStreamLogger'>EventStreamLogger</a>&lt;T, S&gt;",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
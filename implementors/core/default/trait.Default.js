(function() {var implementors = {};
implementors['libc'] = [];implementors['timely'] = ["impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/count_map/struct.CountMap.html' title='timely::progress::count_map::CountMap'>CountMap</a>&lt;T&gt;","impl&lt;T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/frontier/struct.Antichain.html' title='timely::progress::frontier::Antichain'>Antichain</a>&lt;T&gt;","impl&lt;T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/frontier/struct.MutableAntichain.html' title='timely::progress::frontier::MutableAntichain'>MutableAntichain</a>&lt;T&gt;","impl&lt;T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> + <a class='trait' href='timely/progress/timestamp/trait.Timestamp.html' title='timely::progress::timestamp::Timestamp'>Timestamp</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/nested/pointstamp_counter/struct.PointstampCounter.html' title='timely::progress::nested::pointstamp_counter::PointstampCounter'>PointstampCounter</a>&lt;T&gt;","impl&lt;S, T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='enum' href='timely/progress/nested/summary/enum.Summary.html' title='timely::progress::nested::summary::Summary'>Summary</a>&lt;S, T&gt;","impl&lt;TOuter: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a>, TInner: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a>&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/nested/product/struct.Product.html' title='timely::progress::nested::product::Product'>Product</a>&lt;TOuter, TInner&gt;","impl <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/timestamp/struct.RootTimestamp.html' title='timely::progress::timestamp::RootTimestamp'>RootTimestamp</a>","impl <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='timely/progress/timestamp/struct.RootSummary.html' title='timely::progress::timestamp::RootSummary'>RootSummary</a>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/binary_heap/struct.BinaryHeap.html' title='collections::binary_heap::BinaryHeap'>BinaryHeap</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Ord.html' title='core::cmp::Ord'>Ord</a></span>","impl&lt;K, V&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/btree/map/struct.BTreeMap.html' title='collections::btree::map::BTreeMap'>BTreeMap</a>&lt;K, V&gt; <span class='where'>where K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Ord.html' title='core::cmp::Ord'>Ord</a></span>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/btree/set/struct.BTreeSet.html' title='collections::btree::set::BTreeSet'>BTreeSet</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Ord.html' title='core::cmp::Ord'>Ord</a></span>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/linked_list/struct.LinkedList.html' title='collections::linked_list::LinkedList'>LinkedList</a>&lt;T&gt;","impl <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/string/struct.String.html' title='collections::string::String'>String</a>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec/struct.Vec.html' title='collections::vec::Vec'>Vec</a>&lt;T&gt;","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/collections/vec_deque/struct.VecDeque.html' title='collections::vec_deque::VecDeque'>VecDeque</a>&lt;T&gt;","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/alloc/boxed/struct.Box.html' title='alloc::boxed::Box'>Box</a>&lt;T&gt; <span class='where'>where T: <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a></span>","impl&lt;T&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/default/trait.Default.html' title='core::default::Default'>Default</a> for <a class='struct' href='https://doc.rust-lang.org/nightly/alloc/boxed/struct.Box.html' title='alloc::boxed::Box'>Box</a>&lt;<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>[</a>T<a class='primitive' href='https://doc.rust-lang.org/nightly/std/primitive.slice.html'>]</a>&gt;",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()

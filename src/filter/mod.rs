use std::{fmt::{Debug, Display}, sync::{Arc, RwLock}};

pub trait Filter {
    fn do_filter(&mut self, val: serde_json::Value, filter_chain: Box<dyn FilterChain>) -> Option<serde_json::Value>;
}

pub type Link<T> = Option<Arc<RwLock<DefaultFilter<T>>>>; 

#[derive(Debug)]
pub struct DefaultFilter<T> {
    val: T,
    
    next: Link<T>,
}


pub trait FilterChain {
    fn do_filter(&mut self, val:serde_json::Value) -> Option<serde_json::Value>;
}

#[derive(Debug)]
pub struct DefaultFilterChain<T> {
    head: Link<T>,
    
    tail: Link<T>,
}

impl<T> DefaultFilterChain<T> {
    pub fn new() ->Self {
        Self { head: None, tail: None}
    }
    
    pub fn add_filter(&mut self, val: T) {
        
        let new_filter = Arc::new(RwLock::new(DefaultFilter {
            val,
            next: None,
        }));
        
        match self.tail.as_ref() {
            Some(tail) => {
                tail.write().unwrap().next = Some(new_filter.clone());
                self.tail = Some(new_filter);
            },
            None => {
                self.head = Some(new_filter.clone());
                self.tail = Some(new_filter);
            },
        }
    }
}


//impl FilterChain for DefaultFilterChain {
//    fn do_filter(&mut self, val:serde_json::Value) -> Option<serde_json::Value> {
//        if self.current == self.filters.len() {
//            println!("reached end of filter chain. proceeding with next filter chain...");
//            
//            self.next_chain.do_filter(val)
//        } else {
//            
//            let next_filter = self.filters.get_mut(self.current);
//            
//            if let Some(filter) = next_filter {
//                filter.do_filter(val, &mut self)
//            }
//            None
//            
//        }
//        
//    }
//}


#[cfg(test)]
mod tests {
    
    use super::*;
    
    #[test]
    fn test_link_node() {
        let mut filter: DefaultFilterChain<u32> = DefaultFilterChain::new();
        filter.add_filter(2);
        filter.add_filter(3);
        println!("{:#?}", filter);
    }
}
use std::sync::{Arc, RwLock};

pub type EventHandler<EventArgs> = Box<dyn Fn(&EventArgs) + Sync + Send>;

#[derive(Clone)]
pub struct EventEmitter<EventArgs> {
    handlers: Arc<RwLock<Vec<EventHandler<EventArgs>>>>,
}

impl<EventArgs> EventEmitter<EventArgs> {
    pub fn new(handler: impl Fn(&EventArgs) + Sync + Send + 'static) -> Self {
        Self {
            handlers: Arc::new(RwLock::new(vec![Box::new(handler)])),
        }
    }
    pub fn notify(&self, data: &EventArgs) {
        for handler in &*self.handlers.read().unwrap() {
            handler(data);
        }
    }
}

impl<T> Default for EventEmitter<T> {
    fn default() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

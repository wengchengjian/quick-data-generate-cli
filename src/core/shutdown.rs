use tokio::sync::broadcast;

#[derive(Debug)]
pub struct Shutdown {
    pub is_shutdown: bool,

    pub notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Create a new `Shutdown` backed by the given `broadcast::Receiver`.
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;

        self.is_shutdown = true;
    }

    pub fn try_recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        match self.notify.try_recv() {
            Ok(_) => self.is_shutdown = true,
            Err(_) => {}
        }
    }
}

use client::syncer::Syncer;
use std::sync::{Mutex, OnceLock};

use crate::models::Todo;

pub static SYNCER: OnceLock<Mutex<Syncer<Todo>>> = OnceLock::new();

pub struct TodoSyncer;

impl TodoSyncer {
    pub fn global() -> &'static Mutex<Syncer<Todo>> {
        SYNCER.get_or_init(|| Mutex::new(Syncer::new()))
    }
}

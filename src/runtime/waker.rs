use std::{
    cell::UnsafeCell,
    mem,
    sync::atomic::{AtomicU8, Ordering},
    task::Waker,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WakerState {
    Empty = 0,
    Updating = 1,
    Ready = 2,
    Waking = 3,
}

impl From<u8> for WakerState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Empty,
            1 => Self::Updating,
            2 => Self::Ready,
            3 => Self::Waking,
            _ => unreachable!("invalid WakerState"),
        }
    }
}

#[derive(Default)]
pub struct AtomicWaker {
    state: AtomicU8,
    waker: UnsafeCell<Option<Waker>>,
}

unsafe impl Send for AtomicWaker {}
unsafe impl Sync for AtomicWaker {}

impl AtomicWaker {
    pub fn wake(&self) {
        let state: WakerState = self
            .state
            .swap(WakerState::Waking as u8, Ordering::AcqRel)
            .into();

        assert_ne!(state, WakerState::Waking);
        if state == WakerState::Ready {
            mem::replace(unsafe { &mut *self.waker.get() }, None)
                .expect("waker state was Ready without a Waker")
                .wake();
        }
    }

    pub fn update(&self, waker_ref: Option<&Waker>) -> bool {
        let state: WakerState = self.state.load(Ordering::Acquire).into();
        match state {
            WakerState::Empty | WakerState::Ready => {}
            WakerState::Updating => unreachable!("multiple threads trying to update Waker"),
            WakerState::Waking => return false,
        }

        if let Err(new_state) = self.state.compare_exchange(
            state as u8,
            WakerState::Updating as u8,
            Ordering::Acquire,
            Ordering::Acquire,
        ) {
            let new_state: WakerState = new_state.into();
            assert_eq!(new_state, WakerState::Waking);
            return false;
        }

        match mem::replace(
            unsafe { &mut *self.waker.get() },
            waker_ref.map(|waker| waker.clone()),
        ) {
            Some(_dropped_waker) => assert_eq!(state, WakerState::Ready),
            None => assert_eq!(state, WakerState::Empty),
        }

        let new_state = match waker_ref {
            Some(_) => WakerState::Ready,
            None => WakerState::Empty,
        };

        if let Err(new_state) = self.state.compare_exchange(
            WakerState::Updating as u8,
            new_state as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            let new_state: WakerState = new_state.into();
            assert_eq!(new_state, WakerState::Waking);
            unsafe { *self.waker.get() = None };
            return false;
        }

        true
    }
}

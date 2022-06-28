//! Macros

/// getting a reference of SqlExecutor by a given key
macro_rules! gv {
    ($self:expr, $key:expr) => {{
        $self.try_get($key)?.value()
    }};
}

pub(crate) use gv;

/// getting a mutable ref SqlExecutor by a given key
macro_rules! gmv {
    ($self:expr, $key:expr) => {{
        $self.try_get_mut($key)?.value_mut()
    }};
}

pub(crate) use gmv;

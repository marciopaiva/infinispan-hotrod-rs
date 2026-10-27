//! Distributed counters (`docs/adr/0015-distributed-counters.md`).
//! None of these route by key: a counter's name is not a cache key,
//! so every call goes to the seed, same as `query`/`run_exec`. This
//! `RemoteCache` is always already pointed at `org.infinispan.COUNTER`
//! by the time one of these runs (`CounterManager::cache`), never a
//! user's own cache.
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach `call_seed` with no
//! visibility change. No dedicated tests here: every counter
//! operation goes through `call_seed`, already covered by
//! `size_clear_ping_and_stats_always_target_the_seed` in
//! `remote_cache.rs`'s own tests, and the wire-level shape is covered
//! by `connection::counters`'s own tests.

use crate::counter::CounterConfiguration;
use crate::error::Result;

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
    pub(crate) async fn counter_define(
        &self,
        name: String,
        config: CounterConfiguration,
    ) -> Result<bool> {
        match self
            .call_seed(Operation::CounterDefine(name, config))
            .await?
        {
            OperationResult::CounterDefined(created) => Ok(created),
            _ => unreachable!(
                "Operation::CounterDefine always yields OperationResult::CounterDefined"
            ),
        }
    }

    pub(crate) async fn counter_is_defined(&self, name: String) -> Result<bool> {
        match self.call_seed(Operation::CounterIsDefined(name)).await? {
            OperationResult::CounterIsDefined(defined) => Ok(defined),
            _ => unreachable!(
                "Operation::CounterIsDefined always yields OperationResult::CounterIsDefined"
            ),
        }
    }

    pub(crate) async fn counter_get_configuration(
        &self,
        name: String,
    ) -> Result<Option<CounterConfiguration>> {
        match self
            .call_seed(Operation::CounterGetConfiguration(name))
            .await?
        {
            OperationResult::CounterConfiguration(config) => Ok(config),
            _ => unreachable!(
                "Operation::CounterGetConfiguration always yields OperationResult::CounterConfiguration"
            ),
        }
    }

    pub(crate) async fn counter_names(&self) -> Result<Vec<String>> {
        match self.call_seed(Operation::CounterNames).await? {
            OperationResult::CounterNames(names) => Ok(names),
            _ => {
                unreachable!("Operation::CounterNames always yields OperationResult::CounterNames")
            }
        }
    }

    pub(crate) async fn counter_get(&self, name: String) -> Result<i64> {
        match self.call_seed(Operation::CounterGet(name)).await? {
            OperationResult::CounterValue(value) => Ok(value),
            _ => unreachable!("Operation::CounterGet always yields OperationResult::CounterValue"),
        }
    }

    pub(crate) async fn counter_add_and_get(&self, name: String, delta: i64) -> Result<i64> {
        match self
            .call_seed(Operation::CounterAddAndGet(name, delta))
            .await?
        {
            OperationResult::CounterValue(value) => Ok(value),
            _ => unreachable!(
                "Operation::CounterAddAndGet always yields OperationResult::CounterValue"
            ),
        }
    }

    pub(crate) async fn counter_compare_and_swap(
        &self,
        name: String,
        expect: i64,
        update: i64,
    ) -> Result<i64> {
        match self
            .call_seed(Operation::CounterCompareAndSwap(name, expect, update))
            .await?
        {
            OperationResult::CounterValue(value) => Ok(value),
            _ => unreachable!(
                "Operation::CounterCompareAndSwap always yields OperationResult::CounterValue"
            ),
        }
    }

    pub(crate) async fn counter_get_and_set(&self, name: String, value: i64) -> Result<i64> {
        match self
            .call_seed(Operation::CounterGetAndSet(name, value))
            .await?
        {
            OperationResult::CounterValue(value) => Ok(value),
            _ => unreachable!(
                "Operation::CounterGetAndSet always yields OperationResult::CounterValue"
            ),
        }
    }

    pub(crate) async fn counter_reset(&self, name: String) -> Result<()> {
        match self.call_seed(Operation::CounterReset(name)).await? {
            OperationResult::CounterUnit => Ok(()),
            _ => unreachable!("Operation::CounterReset always yields OperationResult::CounterUnit"),
        }
    }

    pub(crate) async fn counter_remove(&self, name: String) -> Result<()> {
        match self.call_seed(Operation::CounterRemove(name)).await? {
            OperationResult::CounterUnit => Ok(()),
            _ => {
                unreachable!("Operation::CounterRemove always yields OperationResult::CounterUnit")
            }
        }
    }
}

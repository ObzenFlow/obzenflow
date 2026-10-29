// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Point-in-time joins against a versioned catalog.

use super::traits::{sealed, ErasedJoinInvocation, JoinBinding, UnifiedJoinHandler};
use super::typed::{
    installed_writer_id, lower_join_outputs, terminal_output_failure, ReferenceProjectionEntry,
};
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::join::config::{
    JoinCatalogKind, JoinReferenceMode, DEFAULT_REFERENCE_BATCH_CAP,
};
use async_trait::async_trait;
use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::provenance::CompositeActivationContext;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, OneFactStageOutput, StageId, TypedPayload, WriterId};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;

/// The catalog version in force at a stream row's time, or why there was none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AsOf<R> {
    /// The version whose effective time is the latest at or before the row's time.
    Found(R),
    /// The key has versions, and the row comes before the first of them.
    BeforeFirstVersion,
    /// The catalog holds no version of the key.
    NoVersions,
}

type Versions<T, R> = BTreeMap<T, ReferenceProjectionEntry<R>>;

/// Read-only typed access to every version of every key in the catalog.
///
/// A `Found` selection privately records the selected version's durable
/// contribution evidence, which the adapter attaches to every fact the
/// invocation returns.
pub struct AsOfReferenceView<'a, K, T, R> {
    projection: &'a HashMap<K, Versions<T, R>>,
    selected_activations: Vec<CompositeActivationContext>,
}

impl<'a, K, T, R> AsOfReferenceView<'a, K, T, R>
where
    K: Eq + Hash,
    T: Ord + Copy,
    R: Clone,
{
    fn new(projection: &'a HashMap<K, Versions<T, R>>) -> Self {
        Self {
            projection,
            selected_activations: Vec::new(),
        }
    }

    /// Select the version of `key` in force at `as_of`, the one whose
    /// effective time is the latest at or before it.
    pub fn select_as_of(&mut self, key: &K, as_of: &T) -> AsOf<R> {
        let Some(versions) = self.projection.get(key) else {
            return AsOf::NoVersions;
        };
        match versions.range(..=*as_of).next_back() {
            Some((_, entry)) => {
                self.selected_activations
                    .extend(entry.activations.iter().cloned());
                AsOf::Found(entry.value.clone())
            }
            None => AsOf::BeforeFirstVersion,
        }
    }

    fn into_selected_activations(self) -> Vec<CompositeActivationContext> {
        self.selected_activations
    }
}

/// Typed point-in-time join whose associated types witness the `join!` arrow.
///
/// Bind an implementation with [`AsOfJoin::new`]. The join loads every catalog
/// version before it reads a stream row, and has no live mode. Point-in-time
/// lookups belong to [`AsOfReferenceView`]; a latest-value join's view has none:
///
/// ```compile_fail,E0599
/// # use obzenflow_core::TypedPayload;
/// # use serde::{Deserialize, Serialize};
/// use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// use obzenflow_runtime::stages::join::{JoinReferenceView, TypedJoinHandler};
/// # #[derive(Clone, Debug, Serialize, Deserialize)]
/// # struct Rate { currency: String, effective_at: u64 }
/// # impl TypedPayload for Rate { const EVENT_TYPE: &'static str = "doc.rate"; }
///
/// struct Price;
///
/// impl TypedJoinHandler for Price {
///     type State = ();
///     type ReferenceKey = String;
///     type Reference = Rate;
///     type Stream = Rate;
///     type Output = Rate;
///
///     fn initial_state(&self) {}
///
///     fn admit_reference(&self, rate: &Rate) -> Result<String, HandlerError> {
///         Ok(rate.currency.clone())
///     }
///
///     fn process_stream(
///         &self,
///         _state: &mut (),
///         references: &mut JoinReferenceView<'_, String, Rate>,
///         row: Rate,
///     ) -> Result<Vec<Rate>, HandlerError> {
///         let _ = references.select_as_of(&row.currency, &row.effective_at);
///         Ok(vec![row])
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "this handler is not a point-in-time join handler",
    label = "this handler does not implement TypedAsOfJoinHandler",
    note = "AsOfJoin::new binds a TypedAsOfJoinHandler; pass a TypedJoinHandler to join! directly"
)]
pub trait TypedAsOfJoinHandler: Send + Sync {
    type State: Clone + Send + Sync;
    type ReferenceKey: Eq + Hash + Clone + Send + Sync + Debug;
    type EffectiveTime: Ord + Copy + Send + Sync + Debug + 'static;
    type Reference: TypedPayload + Clone + Send + Sync + 'static;
    type Stream: TypedPayload + Send + Sync + 'static;
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn initial_state(&self) -> Self::State;

    /// The catalog row's key and the time its version takes effect.
    fn admit_reference(
        &self,
        reference: &Self::Reference,
    ) -> Result<(Self::ReferenceKey, Self::EffectiveTime), HandlerError>;

    fn process_stream(
        &self,
        state: &mut Self::State,
        references: &mut AsOfReferenceView<
            '_,
            Self::ReferenceKey,
            Self::EffectiveTime,
            Self::Reference,
        >,
        stream: Self::Stream,
    ) -> Result<Vec<Self::Output>, HandlerError>;

    /// Returns the facts for the stream's final end of input, and no error.
    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        _references: &mut AsOfReferenceView<
            '_,
            Self::ReferenceKey,
            Self::EffectiveTime,
            Self::Reference,
        >,
    ) -> Vec<Self::Output> {
        Vec::new()
    }

    /// Returns the facts the join publishes as it drains, and no error.
    fn drain(
        &self,
        _state: &Self::State,
        _references: &mut AsOfReferenceView<
            '_,
            Self::ReferenceKey,
            Self::EffectiveTime,
            Self::Reference,
        >,
    ) -> Vec<Self::Output> {
        Vec::new()
    }
}

/// A [`TypedAsOfJoinHandler`] bound for `join!`.
#[derive(Clone, Debug)]
pub struct AsOfJoin<H> {
    handler: H,
}

impl<H: TypedAsOfJoinHandler> AsOfJoin<H> {
    /// Bind `handler` so `join!` runs it as a point-in-time join.
    ///
    /// A latest-value handler is passed to `join!` directly, never bound here:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_core::TypedPayload;
    /// # use serde::{Deserialize, Serialize};
    /// use obzenflow_runtime::stages::join::{left, AsOfJoin};
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Account { id: u64 }
    /// # impl TypedPayload for Account { const EVENT_TYPE: &'static str = "doc.account"; }
    ///
    /// let enrich = AsOfJoin::new(left(
    ///     |account: &Account| account.id,
    ///     |account: &Account| account.id,
    ///     |_reference: Option<Account>, account: Account| account,
    /// ));
    /// ```
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

/// Erased state couples authored state to the runtime-owned versioned
/// projection. Versions and evidence remain inaccessible to authors.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct AsOfJoinState<S, K, T, R> {
    inner: S,
    projection: HashMap<K, Versions<T, R>>,
}

/// Adapter from point-in-time join authoring to the sealed join supervisor surface.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct AsOfJoinHandlerAdapter<H> {
    handler: H,
    lineage: LineagePolicy,
    writer_id: Option<WriterId>,
    stage_names: Option<(String, String)>,
}

impl<H> AsOfJoinHandlerAdapter<H>
where
    H: TypedAsOfJoinHandler,
{
    fn new(handler: H) -> Self {
        Self {
            handler,
            lineage: LineagePolicy::default(),
            writer_id: None,
            stage_names: None,
        }
    }

    fn second_version(
        &self,
        key: &H::ReferenceKey,
        effective_at: &H::EffectiveTime,
    ) -> HandlerError {
        let refused = match &self.stage_names {
            Some((join, catalog)) => {
                format!("join `{join}` refused its catalog `{catalog}` while hydrating")
            }
            None => "join refused its catalog while hydrating".to_string(),
        };
        HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            format!(
                "{refused}: two versions of {key:?} take effect at {effective_at:?}, so a row as \
                 of that time has no single version. Keep one version per key and effective \
                 time, then start a fresh run."
            ),
        ))
    }
}

impl<H> JoinBinding for AsOfJoin<H>
where
    H: TypedAsOfJoinHandler + Clone + Debug + Send + Sync + 'static,
{
    type Reference = H::Reference;
    type Stream = H::Stream;
    type Output = H::Output;
    type Adapter = AsOfJoinHandlerAdapter<H>;

    fn into_adapter(self) -> Self::Adapter {
        AsOfJoinHandlerAdapter::new(self.handler)
    }
}

impl<H> sealed::Sealed for AsOfJoinHandlerAdapter<H> where H: TypedAsOfJoinHandler {}

#[async_trait]
impl<H> UnifiedJoinHandler for AsOfJoinHandlerAdapter<H>
where
    H: TypedAsOfJoinHandler + Send + Sync,
{
    type State = AsOfJoinState<H::State, H::ReferenceKey, H::EffectiveTime, H::Reference>;

    fn install_lineage_policy(&mut self, policy: LineagePolicy) {
        self.lineage = policy;
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn install_stage_names(&mut self, join: &str, catalog: &str) {
        self.stage_names = Some((join.to_string(), catalog.to_string()));
    }

    fn initial_state(&self) -> Self::State {
        AsOfJoinState {
            inner: self.handler.initial_state(),
            projection: HashMap::new(),
        }
    }

    fn process_reference(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        installed_writer_id(self.writer_id)?;
        let reference = H::Reference::try_from_event(&event)
            .map_err(|error| HandlerError::Deserialization(error.to_string()))?;
        let (key, effective_at) = self.handler.admit_reference(&reference)?;
        if state
            .projection
            .get(&key)
            .is_some_and(|versions| versions.contains_key(&effective_at))
        {
            return Err(self.second_version(&key, &effective_at));
        }
        state.projection.entry(key).or_default().insert(
            effective_at,
            ReferenceProjectionEntry {
                value: reference,
                activations: event.composite_activations().to_vec(),
            },
        );
        Ok(Vec::new())
    }

    fn process_stream(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<ErasedJoinInvocation, HandlerError> {
        let writer_id = installed_writer_id(self.writer_id)?;
        let stream = H::Stream::try_from_event(&event)
            .map_err(|error| HandlerError::Deserialization(error.to_string()))?;
        let mut references = AsOfReferenceView::new(&state.projection);
        let outputs = self
            .handler
            .process_stream(&mut state.inner, &mut references, stream)?;
        let selected = references.into_selected_activations();
        lower_join_outputs(
            writer_id,
            self.lineage,
            outputs,
            Some(&event),
            selected,
            None,
        )
    }

    fn reference_mode(&self) -> JoinReferenceMode {
        JoinReferenceMode::FiniteEof
    }

    fn catalog_kind(&self) -> JoinCatalogKind {
        JoinCatalogKind::Versioned
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        Some(DEFAULT_REFERENCE_BATCH_CAP)
    }

    fn on_stream_eof(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, StageFatal> {
        let writer_id = installed_writer_id(self.writer_id).map_err(terminal_output_failure)?;
        let mut references = AsOfReferenceView::new(&state.projection);
        let outputs = self
            .handler
            .on_stream_eof(&mut state.inner, &mut references);
        let selected = references.into_selected_activations();
        let (events, _) = lower_join_outputs(
            writer_id,
            self.lineage,
            outputs,
            Some(&event),
            selected,
            None,
        )
        .map_err(terminal_output_failure)?
        .into_parts();
        Ok(events)
    }

    async fn drain(
        &self,
        state: &Self::State,
        parent: Option<&ChainEvent>,
    ) -> Result<Vec<ChainEvent>, StageFatal> {
        let writer_id = installed_writer_id(self.writer_id).map_err(terminal_output_failure)?;
        let mut references = AsOfReferenceView::new(&state.projection);
        let outputs = self.handler.drain(&state.inner, &mut references);
        let selected = references.into_selected_activations();
        let (events, _) =
            lower_join_outputs(writer_id, self.lineage, outputs, parent, selected, None)
                .map_err(terminal_output_failure)?
                .into_parts();
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::id::CompositeId;
    use obzenflow_core::StageOutputFacts;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Version {
        key: String,
        at: i32,
        value: String,
    }

    impl TypedPayload for Version {
        const EVENT_TYPE: &'static str = "as_of_join.version";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Row {
        key: String,
        at: i32,
    }

    impl TypedPayload for Row {
        const EVENT_TYPE: &'static str = "as_of_join.row";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Selected {
        outcome: String,
    }

    impl TypedPayload for Selected {
        const EVENT_TYPE: &'static str = "as_of_join.selected";
    }

    fn outcome(version: AsOf<Version>) -> String {
        match version {
            AsOf::Found(version) => format!("found {} at {}", version.value, version.at),
            AsOf::BeforeFirstVersion => "before first version".to_string(),
            AsOf::NoVersions => "no versions".to_string(),
        }
    }

    #[derive(Clone, Debug)]
    struct Probe;

    impl TypedAsOfJoinHandler for Probe {
        type State = ();
        type ReferenceKey = String;
        type EffectiveTime = i32;
        type Reference = Version;
        type Stream = Row;
        type Output = Selected;

        fn initial_state(&self) -> Self::State {}

        fn admit_reference(
            &self,
            reference: &Self::Reference,
        ) -> Result<(Self::ReferenceKey, Self::EffectiveTime), HandlerError> {
            Ok((reference.key.clone(), reference.at))
        }

        fn process_stream(
            &self,
            _state: &mut Self::State,
            references: &mut AsOfReferenceView<
                '_,
                Self::ReferenceKey,
                Self::EffectiveTime,
                Self::Reference,
            >,
            stream: Self::Stream,
        ) -> Result<Vec<Self::Output>, HandlerError> {
            Ok(vec![Selected {
                outcome: outcome(references.select_as_of(&stream.key, &stream.at)),
            }])
        }
    }

    fn adapter<B: JoinBinding>(binding: B) -> B::Adapter {
        let mut adapter = binding.into_adapter();
        adapter.install_writer_id(WriterId::from(StageId::new()));
        adapter
    }

    fn scope() -> obzenflow_core::MiddlewareExecutionScope {
        obzenflow_core::MiddlewareExecutionScope::LiveHandler
    }

    fn version(key: &str, at: i32, value: &str) -> ChainEvent {
        Version {
            key: key.to_string(),
            at,
            value: value.to_string(),
        }
        .to_event(WriterId::from(StageId::new()))
    }

    fn row(key: &str, at: i32) -> ChainEvent {
        Row {
            key: key.to_string(),
            at,
        }
        .to_event(WriterId::from(StageId::new()))
    }

    fn admit<H>(
        adapter: &AsOfJoinHandlerAdapter<H>,
        state: &mut <AsOfJoinHandlerAdapter<H> as UnifiedJoinHandler>::State,
        event: ChainEvent,
    ) -> Result<Vec<ChainEvent>, HandlerError>
    where
        H: TypedAsOfJoinHandler,
    {
        adapter.process_reference(
            state,
            event,
            StageId::new(),
            WriterId::from(StageId::new()),
            scope(),
        )
    }

    fn join<H>(
        adapter: &AsOfJoinHandlerAdapter<H>,
        state: &mut <AsOfJoinHandlerAdapter<H> as UnifiedJoinHandler>::State,
        event: ChainEvent,
    ) -> Vec<ChainEvent>
    where
        H: TypedAsOfJoinHandler,
    {
        let (events, framework_eof) = adapter
            .process_stream(
                state,
                event,
                StageId::new(),
                WriterId::from(StageId::new()),
                scope(),
            )
            .expect("a lookup never fails")
            .into_parts();
        assert_eq!(framework_eof, None);
        events
    }

    fn selected(events: &[ChainEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| {
                Selected::try_from_event(event)
                    .expect("typed output")
                    .outcome
            })
            .collect()
    }

    #[test]
    fn a_lookup_takes_the_latest_version_at_or_before_the_row_time() {
        let adapter = adapter(AsOfJoin::new(Probe));
        let mut state = adapter.initial_state();
        for (at, value) in [(10, "ten"), (30, "thirty"), (20, "twenty")] {
            admit(&adapter, &mut state, version("k", at, value)).expect("admitted");
        }

        let outcomes: Vec<String> = [
            ("k", 9),
            ("k", 10),
            ("k", 15),
            ("k", 20),
            ("k", 35),
            ("other", 20),
        ]
        .into_iter()
        .flat_map(|(key, at)| selected(&join(&adapter, &mut state, row(key, at))))
        .collect();
        assert_eq!(
            outcomes,
            [
                "before first version",
                "found ten at 10",
                "found ten at 10",
                "found twenty at 20",
                "found thirty at 30",
                "no versions",
            ]
        );
    }

    #[test]
    fn every_lookup_matches_a_scan_of_the_versions_in_force() {
        let times = [0, 2, 3, 5];
        for mask in 0u32..(1 << times.len()) {
            let present: Vec<i32> = times
                .iter()
                .enumerate()
                .filter(|(index, _)| mask & (1 << index) != 0)
                .map(|(_, time)| *time)
                .collect();
            let mut projection: HashMap<String, Versions<i32, i32>> = HashMap::new();
            for time in &present {
                projection.entry("k".to_string()).or_default().insert(
                    *time,
                    ReferenceProjectionEntry {
                        value: *time,
                        activations: Vec::new(),
                    },
                );
            }
            for as_of in -1..=6 {
                let expected = match present.iter().copied().filter(|time| *time <= as_of).max() {
                    Some(time) => AsOf::Found(time),
                    None if present.is_empty() => AsOf::NoVersions,
                    None => AsOf::BeforeFirstVersion,
                };
                let mut view = AsOfReferenceView::new(&projection);
                assert_eq!(
                    view.select_as_of(&"k".to_string(), &as_of),
                    expected,
                    "versions {present:?} as of {as_of}"
                );
            }
        }
    }

    #[test]
    fn one_time_under_two_keys_and_two_times_under_one_key_are_admitted() {
        let adapter = adapter(AsOfJoin::new(Probe));
        let mut state = adapter.initial_state();
        for event in [
            version("a", 10, "a10"),
            version("b", 10, "b10"),
            version("a", 20, "a20"),
        ] {
            admit(&adapter, &mut state, event).expect("distinct key and time");
        }
        let outcomes = [row("a", 15), row("b", 15), row("a", 25)]
            .into_iter()
            .flat_map(|event| selected(&join(&adapter, &mut state, event)))
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            ["found a10 at 10", "found b10 at 10", "found a20 at 20"]
        );
    }

    #[test]
    fn a_second_version_at_one_key_and_time_is_refused_identical_rows_included() {
        for second in ["changed", "same"] {
            let adapter = adapter(AsOfJoin::new(Probe));
            let mut state = adapter.initial_state();
            admit(&adapter, &mut state, version("k", 10, "same")).expect("first version");
            let error = admit(&adapter, &mut state, version("k", 10, second))
                .expect_err("a second version at one key and time");
            let HandlerError::Fatal(fatal) = error else {
                panic!("the refusal fails the join")
            };
            assert_eq!(fatal.code, StageFatalCode::Protocol);
            assert_eq!(fatal.reason, StageFatalReason::ProtocolInputIntegrity);
            assert_eq!(
                selected(&join(&adapter, &mut state, row("k", 10))),
                ["found same at 10"],
                "the refused row leaves the first version in place"
            );
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct FxRate {
        currency: String,
        effective_at: chrono::NaiveDateTime,
    }

    impl TypedPayload for FxRate {
        const EVENT_TYPE: &'static str = "as_of_join.fx_rate";
    }

    #[test]
    fn the_refusal_names_the_join_the_catalog_the_key_and_the_time() {
        let mut adapter = adapter(crate::stages::join::left_as_of(
            |rate: &FxRate| rate.currency.clone(),
            |rate: &FxRate| rate.effective_at,
            |row: &FxRate| row.currency.clone(),
            |row: &FxRate| row.effective_at,
            |_rate: AsOf<FxRate>, row: FxRate| row,
        ));
        adapter.install_stage_names("price", "fx_rates");
        let mut state = adapter.initial_state();
        let rate = FxRate {
            currency: "EUR".to_string(),
            effective_at: chrono::NaiveDate::from_ymd_opt(2022, 9, 5)
                .and_then(|date| date.and_hms_opt(14, 0, 0))
                .expect("a valid time"),
        };
        let writer = WriterId::from(StageId::new());
        adapter
            .process_reference(
                &mut state,
                rate.clone().to_event(writer),
                StageId::new(),
                writer,
                scope(),
            )
            .expect("first version");
        let error = adapter
            .process_reference(
                &mut state,
                rate.to_event(writer),
                StageId::new(),
                writer,
                scope(),
            )
            .expect_err("a second version at one key and time");
        let HandlerError::Fatal(fatal) = error else {
            panic!("the refusal fails the join")
        };
        assert_eq!(
            fatal.detail,
            "join `price` refused its catalog `fx_rates` while hydrating: two versions of \
             \"EUR\" take effect at 2022-09-05T14:00:00, so a row as of that time has no single \
             version. Keep one version per key and effective time, then start a fresh run."
        );
    }

    fn permutations(items: &[usize]) -> Vec<Vec<usize>> {
        if items.len() <= 1 {
            return vec![items.to_vec()];
        }
        let mut all = Vec::new();
        for (index, item) in items.iter().enumerate() {
            let mut rest = items.to_vec();
            rest.remove(index);
            for mut tail in permutations(&rest) {
                tail.insert(0, *item);
                all.push(tail);
            }
        }
        all
    }

    #[test]
    fn every_order_of_the_same_rows_gives_the_same_lookups_or_the_same_refusal() {
        let rows = [
            ("a", 10, "a10"),
            ("a", 20, "a20"),
            ("b", 5, "b5"),
            ("b", 15, "b15"),
            ("a", 30, "a30"),
        ];
        let queries = [("a", 9), ("a", 25), ("b", 10), ("b", 99), ("c", 1)];
        let mut first: Option<Vec<String>> = None;
        for order in permutations(&[0, 1, 2, 3, 4]) {
            let adapter = adapter(AsOfJoin::new(Probe));
            let mut state = adapter.initial_state();
            for index in &order {
                let (key, at, value) = rows[*index];
                admit(&adapter, &mut state, version(key, at, value)).expect("no ties");
            }
            let outcomes: Vec<String> = queries
                .iter()
                .flat_map(|(key, at)| selected(&join(&adapter, &mut state, row(key, *at))))
                .collect();
            match &first {
                Some(expected) => assert_eq!(&outcomes, expected, "order {order:?}"),
                None => first = Some(outcomes),
            }
        }

        let tied = [
            ("a", 10, "a10"),
            ("b", 5, "b5"),
            ("a", 10, "a10 again"),
            ("a", 20, "a20"),
        ];
        for order in permutations(&[0, 1, 2, 3]) {
            let adapter = adapter(AsOfJoin::new(Probe));
            let mut state = adapter.initial_state();
            let refused = order
                .iter()
                .map(|index| {
                    let (key, at, value) = tied[*index];
                    admit(&adapter, &mut state, version(key, at, value))
                })
                .filter(Result::is_err)
                .count();
            assert_eq!(refused, 1, "order {order:?} refuses the tie once");
        }
    }

    fn activation(event: &ChainEvent, label: &str) -> CompositeActivationContext {
        CompositeActivationContext::new(CompositeId::new("as-of-join:test"), event.id, label, 1)
    }

    fn has_activation(event: &ChainEvent, label: &str) -> bool {
        event
            .composite_activations()
            .iter()
            .any(|activation| activation.entry_port == label)
    }

    #[test]
    fn a_selected_version_passes_its_activations_and_a_plain_version_or_miss_adds_none() {
        let adapter = adapter(AsOfJoin::new(Probe));
        let mut state = adapter.initial_state();
        let evidenced = version("k", 10, "evidenced");
        let label = activation(&evidenced, "selected");
        let evidenced = evidenced
            .try_with_composite_activations(vec![label])
            .expect("valid activation");
        admit(&adapter, &mut state, evidenced).expect("evidenced version");
        admit(&adapter, &mut state, version("k", 20, "plain")).expect("plain version");

        let found = join(&adapter, &mut state, row("k", 15));
        assert_eq!(selected(&found), ["found evidenced at 10"]);
        assert!(has_activation(&found[0], "selected"));

        let plain = join(&adapter, &mut state, row("k", 25));
        assert_eq!(selected(&plain), ["found plain at 20"]);
        assert!(plain[0].composite_activations().is_empty());

        for miss in [row("k", 5), row("other", 15)] {
            let events = join(&adapter, &mut state, miss);
            assert!(events[0].composite_activations().is_empty());
        }
    }

    #[test]
    fn a_point_in_time_join_hydrates_like_a_finite_eof_join_over_a_versioned_catalog() {
        let adapter = adapter(AsOfJoin::new(Probe));
        assert_eq!(adapter.reference_mode(), JoinReferenceMode::FiniteEof);
        assert_eq!(adapter.catalog_kind(), JoinCatalogKind::Versioned);
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Priced {
        value: String,
    }

    impl TypedPayload for Priced {
        const EVENT_TYPE: &'static str = "as_of_join.priced";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Unpriced {
        reason: String,
    }

    impl TypedPayload for Unpriced {
        const EVENT_TYPE: &'static str = "as_of_join.unpriced";
    }

    #[derive(Clone, Debug, StageOutputFacts)]
    enum Pricing {
        Priced(Priced),
        Unpriced(Unpriced),
    }

    #[test]
    fn left_as_of_writes_one_named_output_for_each_stream_row() {
        let adapter = adapter(crate::stages::join::left_as_of(
            |version: &Version| version.key.clone(),
            |version: &Version| version.at,
            |row: &Row| row.key.clone(),
            |row: &Row| row.at,
            |version: AsOf<Version>, _row: Row| match version {
                AsOf::Found(version) => Pricing::Priced(Priced {
                    value: version.value,
                }),
                AsOf::BeforeFirstVersion => Pricing::Unpriced(Unpriced {
                    reason: "before first version".to_string(),
                }),
                AsOf::NoVersions => Pricing::Unpriced(Unpriced {
                    reason: "no versions".to_string(),
                }),
            },
        ));
        let mut state = adapter.initial_state();
        admit(&adapter, &mut state, version("k", 10, "ten")).expect("admitted");

        let found = join(&adapter, &mut state, row("k", 12));
        assert_eq!(found.len(), 1);
        assert_eq!(
            Priced::try_from_event(&found[0]).expect("priced"),
            Priced {
                value: "ten".to_string()
            }
        );
        for (key, at, reason) in [
            ("k", 9, "before first version"),
            ("other", 12, "no versions"),
        ] {
            let missed = join(&adapter, &mut state, row(key, at));
            assert_eq!(missed.len(), 1);
            assert_eq!(
                Unpriced::try_from_event(&missed[0])
                    .expect("unpriced")
                    .reason,
                reason
            );
        }
    }
}

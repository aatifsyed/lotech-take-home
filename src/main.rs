use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    marker::PhantomData,
    ops::RangeInclusive,
    pin::{Pin, pin},
    str::FromStr,
    task::{Context, Poll},
};

use anyhow::{Context as _, anyhow, bail};
use fixed::types::U32F32;
use futures::{
    Stream, StreamExt as _, TryStreamExt as _,
    future::{self, Either},
    stream,
};
use monostate::MustBe;
use num::Zero;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeOwned, IgnoredAny},
};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tracing::{debug, error, info, info_span, instrument::Instrument as _, trace, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    _main().await
}

async fn _main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive("lotech_take_home=debug".parse().unwrap())
                .from_env_lossy(),
        )
        .pretty()
        .init();
    let client = reqwest::Client::new();
    future::join(
        follow::<MustBe!("BNBBTC"), U32F32, U32F32>(
            &client,
            "wss://stream.binance.com:9443/ws/bnbbtc@depth",
            // TODO(aatifsyed): `snap` and `warn_threshold` duplicate the API limit.
            "https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=5000",
            "BNBBTC",
            // > Since depth snapshots retrieved from the API have a limit on the number of price levels
            // > (5000 on each side maximum),
            // > you won't learn the quantities for the levels outside of the initial snapshot unless they change.
            //
            // [binance docs](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly).
            5000,
        ),
        follow::<IgnoredAny, U32F32, U32F32>(
            &client,
            "wss://stream.binance.com:9443/ws/btcusdt@depth",
            "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000",
            "BTCUSDT",
            5000,
        ),
    )
    .await;
}

async fn follow<SymbolT, PriceT, QuantityT>(
    client: &reqwest::Client,
    ws: &str,
    snap: &str,
    symbol: &str,
    warn_threshold: usize,
) -> !
where
    PriceT: Ord + Clone + fmt::Display + Debug + PartialEq + FromStr,
    PriceT::Err: fmt::Display,
    QuantityT: Zero + FromStr + Debug,
    QuantityT::Err: fmt::Display,
    SymbolT: Debug + DeserializeOwned,
{
    _follow(
        || depth_updates::<SymbolT, PriceT, QuantityT>(ws),
        || async {
            Ok(client
                .get(snap)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?)
        },
        warn_threshold,
    )
    .instrument(info_span!("follow", symbol))
    .await
}

/// The functional core of the application.
/// This is NOT testable as written,
/// and would require rewriting one of the following to get there:
/// - a observe/break path out of the infinite loop.
/// - a core/shell pattern
///
/// This is a faithful transcription of [the binance docs](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly).
///
/// This does NOT handle the case of a trivial disconnection from the event stream
/// in the most efficient way - it rebuilds the orderbook every time.
async fn _follow<MakeStreamFn, FetchSnapshotFn, DepthUpdateSt, SymbolT, PriceT, QuantityT>(
    mut make_stream: MakeStreamFn,
    mut fetch_snapshot: FetchSnapshotFn,
    warn_threshold: usize,
) -> !
where
    MakeStreamFn: FnMut() -> DepthUpdateSt,
    DepthUpdateSt: Stream<Item = anyhow::Result<DepthUpdate<SymbolT, PriceT, QuantityT>>>,
    FetchSnapshotFn: AsyncFnMut() -> anyhow::Result<Snapshot<PriceT, QuantityT>>,
    PriceT: Ord + fmt::Display,
    QuantityT: Zero,
    PriceT: Clone,
    PriceT: Debug,
{
    loop {
        // 1. Open a WebSocket connection to wss://stream.binance.com:9443/ws/bnbbtc@depth.
        let mut stream = pin!(
            make_stream()
                .scan(None, |state, res| {
                    let res = check_event_ixs(state, res);
                    async move { res } // this phrasing is required to free lifetime 'state
                })
                .peekable()
        );
        // 2. Buffer the events received from the stream. Note the U of the first event you received.
        debug!("start stream");
        let stream_start = match stream.as_mut().peek().await {
            Some(Ok(upd)) => *upd.ixs.start(),
            Some(Err(e)) => {
                let error = &**e;
                error!(error, "stream failed");
                continue;
            }
            None => {
                error!("stream ended");
                continue;
            }
        };
        // 3. Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=5000.
        // 4. If the lastUpdateId from the snapshot is strictly less than the U from step 2, go back to step 3.
        let snapshot = loop {
            debug!("fetch snapshot");
            match fetch_snapshot().await {
                Ok(snap) if snap.ix < stream_start => {
                    debug!(snapshot = snap.ix, stream_start, "snapshot predates streem")
                }
                Ok(snap) => {
                    if snap.asks.len() > warn_threshold {
                        warn!("may have missed asks in snapshot")
                    }
                    if snap.bids.len() > warn_threshold {
                        warn!("may have missed bids in snapshot")
                    }
                    break snap;
                }
                Err(e) => {
                    let error = &*e;
                    error!(error, "couldn't fetch snapshot")
                }
            }
        };
        debug!(snapshot.ix);
        // 5. In the buffered events, discard any event where u is <= lastUpdateId of the snapshot.
        //    The first buffered event should now have lastUpdateId within its [U;u] range.
        let mut stream = pin!(stream.try_skip_while(move |upd| {
            let should_skip = *upd.ixs.end() <= snapshot.ix;
            async move { Ok(should_skip) }
        }));
        // 6. Set your local order book to the snapshot. Its update ID is lastUpdateId.
        let mut orderbook = Orderbook::from(snapshot);
        let mut current_spread = None::<(PriceT, PriceT)>;
        // 7. Apply the update procedure below to all buffered events, and then to all subsequent events received.

        loop {
            match stream.next().await {
                Some(Ok(upd)) => {
                    let update_indices = upd.ixs;
                    let orderbook_ix = orderbook.update_ix;
                    // 1. If the event u (last update ID) is < the update ID of your local order book, ignore the event.
                    if update_indices.end() < &orderbook.update_ix {
                        trace!(?update_indices, orderbook_ix, "ignoring early event");
                        continue;
                    }
                    // 2. If the event U (first update ID) is > the update ID of your local order book, something went wrong.
                    //   Discard your local order book and restart the process from the beginning.
                    if update_indices.start() > &(orderbook.update_ix + 1) {
                        //         NOTE: deviation from protocol text ^^^
                        error!(?update_indices, orderbook_ix, "out-of-order event");
                        continue;
                    }
                    for (side, levels) in [
                        (&mut orderbook.bids, upd.bids),
                        (&mut orderbook.asks, upd.asks),
                    ] {
                        // 3. For each price level in bids (b) and asks (a), set the new quantity in the order book:
                        //   - If the price level does not exist in the order book, insert it with new quantity.
                        //   - If the quantity is zero, remove the price level from the order book.
                        for (price, qty) in levels {
                            // BTreeMap has no concept of reservation,
                            // so this explicit for-loop doesn't cost anything.
                            match qty.is_zero() {
                                true => side.remove(&price),
                                false => side.insert(price, qty),
                            };
                        }
                    }
                    // 4. Set the order book update ID to the last update ID (u) in the processed event.
                    orderbook.update_ix = *update_indices.end();

                    if let (Some((bid, _)), Some((ask, _))) = (
                        orderbook.bids.last_key_value(),
                        orderbook.asks.first_key_value(),
                    ) {
                        if bid > ask {
                            error!(%bid, %ask, "orderbook has been crossed");
                            break;
                        }

                        let spread = (bid.clone(), ask.clone());
                        if current_spread.is_none_or(|it| it != spread) {
                            info!(%bid, %ask, num_bids = orderbook.bids.len(), num_asks = orderbook.asks.len(), "new spread");
                        }
                        current_spread = Some(spread);
                    }
                }
                Some(Err(e)) => {
                    let error = &*e;
                    error!(error, "stream failed");
                    break;
                }
                None => {
                    error!("stream ended");
                    break;
                }
            }
        }
    }
}

/// Yield an [`Err`] if we miss any [`EventIx`]s on a stream of [`DepthUpdate`]s.
fn check_event_ixs<SymbolT, PriceT, QuantityT>(
    state: &mut Option<EventIx>,
    res: anyhow::Result<DepthUpdate<SymbolT, PriceT, QuantityT>>,
) -> Option<anyhow::Result<DepthUpdate<SymbolT, PriceT, QuantityT>>> {
    match res {
        Ok(depth_update) => {
            let start = *depth_update.ixs.start();
            let end = *depth_update.ixs.end();
            if start > end {
                return Some(Err(anyhow!("bad event indices on wire: {start} > {end}")));
            }
            if let Some(prev_end) = state {
                if start != (*prev_end + 1) {
                    return Some(Err(anyhow!(
                        "missing events on wire between {prev_end} and {start}"
                    )));
                }
            }
            *state = Some(*depth_update.ixs.end());
            Some(Ok(depth_update))
        }
        Err(e) => Some(Err(e)),
    }
}

struct Orderbook<PriceT, QuantityT> {
    update_ix: EventIx,
    /// Map value must never be zero.
    bids: BTreeMap<PriceT, QuantityT>,
    /// Map value must never be zero.
    asks: BTreeMap<PriceT, QuantityT>,
}

impl<PriceT: Ord, QuantityT: Zero> From<Snapshot<PriceT, QuantityT>>
    for Orderbook<PriceT, QuantityT>
{
    fn from(Snapshot { ix, bids, asks }: Snapshot<PriceT, QuantityT>) -> Self {
        Self {
            update_ix: ix,
            bids: bids.into_iter().filter(|(_, qty)| !qty.is_zero()).collect(),
            asks: asks.into_iter().filter(|(_, qty)| !qty.is_zero()).collect(),
        }
    }
}

/// Open a websocket stream against `remote`, parsing returned messages as [`DepthUpdate`]s.
///
/// Always yields at least one item.
fn depth_updates<SymbolT, PriceT, QuantityT>(
    remote: impl IntoClientRequest,
) -> impl Stream<Item = anyhow::Result<DepthUpdate<SymbolT, PriceT, QuantityT>>>
where
    SymbolT: DeserializeOwned + Debug,
    PriceT: FromStr + Debug,
    PriceT::Err: fmt::Display,
    QuantityT: FromStr + Debug,
    QuantityT::Err: fmt::Display,
{
    // early evaluation of `remote` relaxes the `Unpin` bound
    // TODO(aatifsyed): upstream writing like this to tokio_tungstenite
    match remote.into_client_request() {
        Ok(req) => {
            let uri = req.uri();
            let span = info_span!("depth_updates", %uri);
            let stream = stream::once(tokio_tungstenite::connect_async(req))
                .map(|res| {
                    res.context("failed to dial server")
                        .map(|(msgs, _http_response)| {
                            msgs.map(|e| e.context("bad message on socket"))
                        })
                })
                .try_flatten()
                .inspect_ok(|message| trace!(?message, "message on socket"))
                .try_filter_map(|msg| async {
                    match msg {
                        Message::Close(Some(reason)) => bail!("stream closed: {reason}"),
                        Message::Text(s) => Ok(Some(
                            deserialize(&mut serde_json::Deserializer::from_str(&s))
                                .context("couldn't deserialize text on socket")?,
                        )),
                        Message::Binary(b) => Ok(Some(
                            deserialize(&mut serde_json::Deserializer::from_slice(&b))
                                .context("couldn't deserialize bytes on socket")?,
                        )),
                        Message::Close(None) => bail!("stream closed"),
                        Message::Ping(_) | Message::Pong(_) => Ok(None),
                        Message::Frame(frame) => unreachable!("unexpected raw frame: {frame:?}"),
                    }
                })
                .inspect_ok(|update| trace!(?update, "depth update"));
            Either::Right(InstrumentedStream::new(stream, span))
        }
        Err(e) => Either::Left(stream::once(async move {
            Err(anyhow::Error::from(e).context("couldn't create outbound request"))
        })),
    }
}

/// Provide additional information if `debug_assertions` are enabled.
fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(d: D) -> anyhow::Result<T>
where
    D::Error: Send + Sync + 'static,
{
    Ok(match cfg!(debug_assertions) {
        true => serde_path_to_error::deserialize(d)?,
        false => T::deserialize(d)?,
    })
}

/// Monotonically increasing number assigned to successive events against a book.
type EventIx = u64;

/// See [binance docs](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#order-book).
#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
#[serde(bound(deserialize = "
    PriceT: FromStr,
    PriceT::Err: fmt::Display,
    QuantityT: FromStr,
    QuantityT::Err: fmt::Display,
"))]
struct Snapshot<PriceT, QuantityT> {
    #[serde(rename = "lastUpdateId")]
    ix: EventIx,
    #[serde(deserialize_with = "levels")]
    bids: Vec<(PriceT, QuantityT)>,
    #[serde(deserialize_with = "levels")]
    asks: Vec<(PriceT, QuantityT)>,
}

#[test]
fn snapshot() {
    serde_json::from_str::<Snapshot<U32F32, U32F32>>(include_str!("../snapshot.json")).unwrap();
}

/// See [binance docs](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream).
///
/// The number of [`ids`](Self::ids) is greater than or equal to the total length
/// of [`bids`](Self::bids) and [`asks`](Self::asks).
///
/// It is not possible to correlate any event ID to any particular price level.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DepthUpdate<SymbolT, PriceT, QuantityT> {
    symbol: SymbolT,
    bids: Vec<(PriceT, QuantityT)>,
    asks: Vec<(PriceT, QuantityT)>,
    /// The start is documented as `U`, and the end as `u`.
    ixs: RangeInclusive<EventIx>,
}

#[test]
fn depth_update() {
    for line in include_str!("../stream.ndjson").lines() {
        serde_json::from_str::<DepthUpdate<MustBe!("BNBBTC"), U32F32, U32F32>>(line).unwrap();
    }
}

/// Deserialize a `["123", "456"]` list via [`FromStr`], without allocating intermediate strings.
///
/// `(PriceT, QuantityT)` is clear enough in our code that we don't need a `Level` struct.
/// Just keep using the tuple.
fn levels<'de, PriceT, QuantityT, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<(PriceT, QuantityT)>, D::Error>
where
    PriceT: FromStr,
    PriceT::Err: fmt::Display,
    QuantityT: FromStr,
    QuantityT::Err: fmt::Display,
{
    #[derive(Deserialize)]
    #[serde(bound(deserialize = "
        PriceT: FromStr,
        PriceT::Err: fmt::Display,
        QuantityT: FromStr,
        QuantityT::Err: fmt::Display,
    "))]
    struct Level<PriceT, QuantityT>(
        #[serde(deserialize_with = "from_str")] PriceT,
        #[serde(deserialize_with = "from_str")] QuantityT,
    );

    /// Most implementation of this kind of function allocate a string first,
    /// but since we know we're parsing numbers _without_ special characters,
    /// we can avoid the allocation.
    fn from_str<'de, T, D: Deserializer<'de>>(d: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        struct Visitor<T>(PhantomData<T>);
        impl<T> serde::de::Visitor<'_> for Visitor<T>
        where
            T: FromStr,
            T::Err: fmt::Display,
        {
            type Value = T;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_fmt(format_args!(
                    "a string that can be parsed as a {}",
                    std::any::type_name::<T>()
                ))
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse().map_err(serde::de::Error::custom)
            }
        }
        d.deserialize_str(Visitor(PhantomData))
    }

    // we're somewhat relying on this recollection to be a no-op
    Ok(Vec::<Level<PriceT, QuantityT>>::deserialize(d)?
        .into_iter()
        .map(|Level(prc, qty)| (prc, qty))
        .collect())
}

/// Breaking this impl block out allows us to check the event type.
impl<'de, SymbolT, PriceT, QuantityT> Deserialize<'de> for DepthUpdate<SymbolT, PriceT, QuantityT>
where
    SymbolT: Deserialize<'de>,
    PriceT: FromStr,
    PriceT::Err: fmt::Display,
    QuantityT: FromStr,
    QuantityT::Err: fmt::Display,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(bound(deserialize = "
            SymbolT: Deserialize<'de>,

            PriceT: FromStr,
            PriceT::Err: fmt::Display,
            QuantityT: FromStr,
            QuantityT::Err: fmt::Display,
        "))]
        struct _DepthUpdate<SymbolT, PriceT, QuantityT> {
            #[cfg(debug_assertions)]
            #[allow(unused)]
            e: MustBe!("depthUpdate"),
            // we ignore the timestamp, but if we really wanted it...
            //
            // #[serde(deserialize_with = "unix_millis", rename = "E")]
            // time: jiff::Timestamp,
            #[serde(rename = "s")]
            symbol: SymbolT,
            #[serde(rename = "U")]
            ix0: EventIx,
            #[serde(rename = "u")]
            ixn: EventIx,
            #[serde(deserialize_with = "levels", rename = "b")]
            bids: Vec<(PriceT, QuantityT)>,
            #[serde(deserialize_with = "levels", rename = "a")]
            asks: Vec<(PriceT, QuantityT)>,
        }
        let _DepthUpdate {
            e: _,
            symbol,
            ix0,
            ixn,
            bids,
            asks,
        } = _DepthUpdate::deserialize(deserializer)?;
        Ok(DepthUpdate {
            symbol,
            bids,
            asks,
            ixs: ix0..=ixn,
        })
    }
}

pin_project_lite::pin_project! {
/// [`tracing::instrument::Instrumented`], but for [`Stream`]s.
struct InstrumentedStream<S> {
    #[pin] stream: S,
    span: tracing::Span
}}

impl<S> InstrumentedStream<S> {
    pub fn new(stream: S, span: tracing::Span) -> Self {
        Self { stream, span }
    }
}

impl<S: Stream> Stream for InstrumentedStream<S> {
    type Item = S::Item;
    /// To achieve a similar effect inline in [`depth_updates`],
    /// we have to clone the outer span a bunch,
    /// and write code that rustfmt refuses to prettify...
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.span.enter();
        this.stream.poll_next(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

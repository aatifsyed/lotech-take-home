use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    ops::RangeInclusive,
    pin::{Pin, pin},
    str::FromStr,
    task::{Context, Poll},
};

use anyhow::{Context as _, bail};
use futures::{Stream, StreamExt as _, TryStreamExt as _, future::Either, stream};
use monostate::MustBe;
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tracing::{debug, error, info_span, trace};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    _main().await
}

async fn _main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .pretty()
        .init();
    let mut st = pin!(depth_updates::<MustBe!("BNBBTC"), String, String>(
        "wss://stream.binance.com:9443/ws/bnbbtc@depth",
    ));
    while let Some(it) = st.next().await {
        let DepthUpdate {
            bids,
            asks,
            ixs: ids,
            ..
        } = it.unwrap();
        println!(
            "{}..={} = {}\t{}+{} = {}",
            ids.start(),
            ids.end(),
            ids.clone().count(),
            bids.len(),
            asks.len(),
            bids.len() + asks.len()
        )
    }
}

async fn orderbook<
    MakeStreamF,
    FetchSnapshotF,
    DepthUpdateSt,
    FetchSnapshotFut,
    SymbolT,
    PriceT,
    QuantityT,
>(
    mut make_stream: MakeStreamF,
    mut fetch_snapshot: FetchSnapshotF,
) where
    MakeStreamF: FnMut() -> DepthUpdateSt,
    FetchSnapshotF: FnMut() -> FetchSnapshotFut,
    DepthUpdateSt: Stream<Item = anyhow::Result<DepthUpdate<SymbolT, PriceT, QuantityT>>>,
    FetchSnapshotFut: Future<Output = anyhow::Result<Snapshot<PriceT, QuantityT>>>,
{
    'stream: loop {
        let stream = pin!(make_stream().peekable());
        match stream.peek().await {
            Some(Ok(first)) => todo!(),
            Some(Err(e)) => {
                let error: &dyn std::error::Error = e.as_ref();
                error!(error, "failed to start stream, retrying...");
                break 'stream;
            }
            None => {
                error!("failed to start stream, retrying...");
                break 'stream;
            }
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
    // TODO(aatifsyed): upstream ^this to tokio_tungstenite
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
                .inspect_ok(|update| debug!(?update, "depth update"));
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
type EventIx = u32;

/// See [binance docs](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#order-book).
#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
struct Snapshot<PriceT, QuantityT> {
    ix: EventIx,
    bids: Vec<(PriceT, QuantityT)>,
    asks: Vec<(PriceT, QuantityT)>,
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

/// We want to use [`fixed`]-point numeric types most of the time,
/// but their [`serde`] impls don't cover JSON strings
/// (which is how Binance expose their numbers).
///
/// Rather than create shim numeric types,
/// which are always a hassle,
/// go via [`FromStr`] (avoiding extraneous allocations),
/// since _whichever_ numeric library we use,
/// it should definitely have those impls.
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
            PriceT: FromStr,
            PriceT::Err: fmt::Display,
            QuantityT: FromStr,
            QuantityT::Err: fmt::Display,
        "))]
        struct Level<PriceT, QuantityT>(
            #[serde(deserialize_with = "from_str")] PriceT,
            #[serde(deserialize_with = "from_str")] QuantityT,
        );

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
            #[serde(rename = "b")]
            bids: Vec<Level<PriceT, QuantityT>>,
            #[serde(rename = "a")]
            asks: Vec<Level<PriceT, QuantityT>>,
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
            // these recollections shouldn't reallocate
            bids: bids
                .into_iter()
                .map(|Level(price, qty)| (price, qty))
                .collect(),
            asks: asks
                .into_iter()
                .map(|Level(price, qty)| (price, qty))
                .collect(),
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

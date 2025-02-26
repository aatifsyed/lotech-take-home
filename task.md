# LO Tech Quant Dev Test

1. Objective:
  Connect to Binance’s API to fetch orderbook data for multiple cryptocurrency pairs. Maintain a local orderbook that updates dynamically based on the WebSocket feed provided by Binance.
2. API Reference:
  Use Binance’s Diff. Depth Stream WebSocket API to receive incremental updates on the orderbook for specific pairs.
  - API Docs: [Binance Diff. Depth Stream](https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream)
  - Candidates are encouraged to read and understand the steps outlined in the API documentation on maintaining a local orderbook.
3. Requirements:
  - **Connection**: Establish a connection with Binance’s WebSocket API.
  - **Orderbook Management**: Use the WebSocket data to maintain a local, synchronized orderbook for multiple cryptocurrency pairs (e.g., BTCUSDT, ETHUSDT, etc.).
  - **Resilience**: Ensure the orderbook remains consistent with Binance’s data despite connection issues.

---

# Key Aspects to Address

The following questions should guide candidates in developing a robust solution and offer insight into their depth of understanding.

1. **Validating the Orderbook**
  How will you verify the accuracy of your local orderbook?
2. **Handling Invalid Orderbooks**
  What happens if your local orderbook becomes desynchronized?
3. **Reconnecting After a Dropped Connection**
  How will your program handle reconnection after a dropped WebSocket connection?
4. **Testing and Validating Code**
  How can you test the reliability of your orderbook maintenance logic?
5. **Performance**
  How can you measure the performance of your code?

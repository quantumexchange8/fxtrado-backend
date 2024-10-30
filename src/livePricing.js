// livePricing.js
import fastify from 'fastify';

const updateLiveOHLC = async (fastify) => {
  try {
    // Retrieve the latest tick data for each symbol
    const [latestTicks] = await fastify.mysql.query(`
      SELECT symbol, bid, ask, Date 
      FROM ticks 
      WHERE (symbol, Date) IN 
            (SELECT symbol, MAX(Date) FROM ticks GROUP BY symbol)
    `);

    const currentTime = Math.floor(Date.now() / 1000); // Get the current UNIX timestamp in seconds

    // Iterate over each symbol's latest tick data
    for (const tick of latestTicks) {
      const { symbol, bid, ask } = tick;

      // Get the existing entry for the symbol in live_ohlc
      const [currentOHLC] = await fastify.mysql.query(`
        SELECT open, high, low, close, date 
        FROM live_ohlc 
        WHERE symbol = ?
      `, [symbol]);

      const ohlcRow = currentOHLC[0];

      if (!ohlcRow) {
        // If there's no current OHLC data for this symbol, initialize it
        await fastify.mysql.query(`
          INSERT INTO live_ohlc (symbol, date, open, high, low, close) 
          VALUES (?, ?, ?, ?, ?, ?)
        `, [symbol, currentTime, bid, bid, bid, bid]);
      } else {
        const { open, high, low, close, date } = ohlcRow;
        // console.log('1', high)

        // Check if a new minute has started
        // console.log(Math.floor(date / 60) !== Math.floor(currentTime / 60))
        if (Math.floor(date / 1000 / 60) !== Math.floor(currentTime / 60)) {
          // console.log('1')
          // Save the previous candle to the history table
          // await fastify.mysql.query(`
          //   INSERT INTO history_charts (symbol, date, open, high, low, close) 
          //   VALUES (?, ?, ?, ?, ?, ?)
          // `, [symbol, date, open, high, low, close]);

          // Reset for the new minute
          await fastify.mysql.query(`
            UPDATE live_ohlc 
            SET date = FROM_UNIXTIME(?), open = ?, high = GREATEST(high, ?), low = LEAST(low, ?), close = ? 
            WHERE symbol = ?
          `, [currentTime, bid, bid, bid, bid, symbol]);
        } else {
          // Update the existing candle data
          // console.log('2')
          await fastify.mysql.query(`
            UPDATE live_ohlc 
            SET 
              high = GREATEST(high, ?),          -- Update 'high' if the new bid is higher
              low = LEAST(low, ?),                -- Update 'low' if the new bid is lower
              close = ?                           -- Always update 'close' to the latest bid
            WHERE symbol = ?
          `, [bid, bid, bid, symbol]);
        }
      }
    }
  } catch (error) {
    console.error("Error updating live OHLC data:", error);
  }
};

// Schedule the function to run every second for real-time updates
export const schedeEveryMinOHLC = (fastify) => {
  setInterval(async () => {
    await updateLiveOHLC(fastify);
  }, 2000); // Runs every second
};

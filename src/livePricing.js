// livePricing.js
import fastify from 'fastify';

const updateLiveOHLC = async (fastify) => {
  // Retrieve the latest tick data (latest bid and ask prices for each symbol)
  const [latestTicks] = await fastify.mysql.query(
    `SELECT symbol, bid, ask, Date 
     FROM ticks 
     WHERE Date = (SELECT MAX(Date) FROM ticks WHERE symbol = ticks.symbol)`
  );

  for (let tick of latestTicks) {
    const { symbol, bid, ask, Date } = tick;

    // Check if there's an entry for the current minute
    const [existingRow] = await fastify.mysql.query(
      `SELECT open, high, low, close, date FROM live_ohlc 
       WHERE symbol = ? AND DATE_FORMAT(date, '%Y-%m-%d %H:%i') = DATE_FORMAT(?, '%Y-%m-%d %H:%i')`,
      [symbol, Date]
    );

    if (existingRow) {
      // If an entry exists, update high, low, and close values
      await fastify.mysql.query(
        `UPDATE live_ohlc SET
          high = GREATEST(high, ?),
          low = LEAST(low, ?),
          close = ?
        WHERE symbol = ? AND DATE_FORMAT(date, '%Y-%m-%d %H:%i') = DATE_FORMAT(?, '%Y-%m-%d %H:%i')`,
        [bid, bid, ask, symbol, Date]
      );
    } else {
      // If no entry exists for the minute, initialize with the current tick data
      await fastify.mysql.query(
        `INSERT INTO live_ohlc (date, symbol, open, high, low, close)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [Date, symbol, bid, bid, bid, ask]
      );
    }
  }
};

// Schedule the function to run every second to ensure real-time updates
export const schedeEveryMinOHLC = (fastify) => {
  setInterval(async () => {
    await updateLiveOHLC(fastify);
  }, 1000); // Runs every second
};

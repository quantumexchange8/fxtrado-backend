export const startVolumeCreation = (fastify) => {
  // Run every minute
  setInterval(async () => {
    let connection;
    try {
      connection = await fastify.mysql.getConnection();

      // Fetch all active forex pairs in a single query
      const [forexPairs] = await connection.query(
        'SELECT symbol_pair FROM forex_pairs WHERE status = "active"'
      );

      // Array to store bulk insert data
      const insertData = [];

      // Process each forex pair and prepare OHLC data for insertion
      const promises = forexPairs.map(async ({ symbol_pair }) => {
        try {
          // Fetch OHLC data for the last 1 minute
          const [ohlcData] = await connection.query(
            `SELECT 
                (SELECT Bid FROM ticks 
                 WHERE Symbol = ? 
                 AND Date BETWEEN DATE_SUB(DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND), INTERVAL 1 MINUTE)
                   AND DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)
                 ORDER BY Date ASC LIMIT 1) AS open,
                
                MAX(GREATEST(Bid, Ask)) AS high,
                MIN(LEAST(Bid, Ask)) AS low,
                
                (SELECT Bid FROM ticks 
                 WHERE Symbol = ? 
                 AND Date BETWEEN DATE_SUB(DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND), INTERVAL 1 MINUTE)
                   AND DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)
                 ORDER BY Date DESC LIMIT 1) AS close
              FROM ticks
              WHERE Symbol = ? 
                AND Date BETWEEN DATE_SUB(DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND), INTERVAL 1 MINUTE)
               AND DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND);
            `,
            [symbol_pair, symbol_pair, symbol_pair]
          );

          // If OHLC data is present, prepare it for insertion
          if (ohlcData && ohlcData.length) {
            const { open, high, low, close } = ohlcData[0];
            const currentDate = new Date();
            currentDate.setUTCSeconds(0, 0);
            currentDate.setUTCMinutes(currentDate.getUTCMinutes() - 1);
            const date = formatDate(currentDate);

            // Check for duplicates before adding to insert data
            const [existing] = await connection.query(
              `SELECT 1 FROM history_charts WHERE Symbol = ? AND Date = ? LIMIT 1`,
              [symbol_pair, date]
            );

            // If there's no duplicate, add to insert data
            if (!existing.length && open !== null && high !== null && low !== null && close !== null) {
              insertData.push([date, open, high, low, close, symbol_pair]);
            }
          }
        } catch (err) {
          console.error(`Error processing currency pair ${symbol_pair}:`, err);
        }
      });

      // Wait for all forex pairs to be processed
      await Promise.all(promises);

      // If there's data to insert, perform bulk insertion
      if (insertData.length > 0) {
        await connection.beginTransaction(); // Begin transaction
        await connection.query(
          `
          INSERT INTO history_charts (Date, Open, High, Low, Close, Symbol)
          VALUES ?
        `,
          [insertData]
        );
        await connection.commit(); // Commit transaction
        console.log(`Inserted ${insertData.length} 1-min candlestick records.`);
      } else {
        console.log('No new candlestick data to insert.');
      }
    } catch (err) {
      console.error('Error processing candlestick data:', err);
      if (connection) await connection.rollback(); // Rollback if error occurs
    } finally {
      // Release connection in the finally block
      if (connection) connection.release();
    }
  }, 60000); // Interval set to 1 minute
};

// Utility function to format the date in 'YYYY-MM-DD HH:mm:ss.SSS' format
const formatDate = (date) => {
  return (
    date.getUTCFullYear() +
    '-' +
    String(date.getUTCMonth() + 1).padStart(2, '0') +
    '-' +
    String(date.getUTCDate()).padStart(2, '0') +
    ' ' +
    String(date.getUTCHours()).padStart(2, '0') +
    ':' +
    String(date.getUTCMinutes()).padStart(2, '0') +
    ':' +
    String(date.getUTCSeconds()).padStart(2, '0') +
    '.00'
  );
};

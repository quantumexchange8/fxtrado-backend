export const startVolumeCreation = (fastify) => {
  // Run every 15 seconds
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

      

      const promises = forexPairs.map(async ({ symbol_pair }) => {
        try {
          // Fetch OHLC data for the last 15 seconds
          const [ohlcData] = await connection.query(
            ` SELECT 
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

          // If data is present, add it to the insert array
          if (ohlcData && ohlcData.length) {
            const { open, high, low, close } = ohlcData[0];

            const currentDate = new Date();
            // Adjust to the last full minute
            currentDate.setUTCSeconds(0, 0); // Set seconds and milliseconds to zero
            currentDate.setUTCMinutes(currentDate.getUTCMinutes() - 1); // Subtract one minute
            const date = formatDate(currentDate);

            // const currentDate = new Date();
            // const date = formatDate(currentDate);

            // console.log('OHLC Data:', { open, high, low, close, date, symbol_pair });
            
            // Check if all necessary data is present before inserting
            if (open !== null && high !== null && low !== null && close !== null) {
              
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
        await connection.query(
          `
          INSERT INTO history_charts (Date, Open, High, Low, Close, Symbol)
          VALUES ?
        `,
          [insertData]
        );

        console.log(`Inserted ${insertData.length} 1-min candlestick records.`);
      } else {
        console.log('No new candlestick data to insert.');
      }
    } catch (err) {
      console.error('Error processing candlestick data:', err);
    } finally {
      // Release connection in the finally block to ensure it's released even if an error occurs
      if (connection) connection.release();
    }
  }, 60000); // Interval set to 1 minutes
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

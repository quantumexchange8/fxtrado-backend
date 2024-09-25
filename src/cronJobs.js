import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  cron.schedule('* * * * *', async () => {
    let connection;
    try {
      connection = await fastify.mysql.getConnection();

      // Fetch all active forex pairs in a single query
      const [forexPairs] = await connection.query('SELECT currency_pair FROM forex_pairs WHERE status = "active"');

      // Array to store bulk insert data
      const insertData = [];

      const promises = forexPairs.map(async ({ currency_pair }) => {
        try {
          // Fetch OHLC data for the last 1 minute
          const [ohlcData] = await connection.query(`
            SELECT 
              (SELECT Bid FROM ticks 
               WHERE Symbol = ? 
               AND Date >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') 
               AND Date < DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00') 
               ORDER BY Date ASC LIMIT 1) AS open,
      
              MAX(GREATEST(Bid, Ask)) AS high,
              MIN(LEAST(Bid, Ask)) AS low,
      
              (SELECT Bid FROM ticks 
               WHERE Symbol = ? 
               AND Date >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00') 
               AND Date < DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00') 
               ORDER BY Date DESC LIMIT 1) AS close
      
            FROM ticks
            WHERE Symbol = ? 
              AND Date >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MINUTE), '%Y-%m-%d %H:%i:00')
              AND Date < DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00');
          `, [currency_pair, currency_pair, currency_pair]);
      
          // If data is present, add it to the insert array
          if (ohlcData && ohlcData.length) {
            const { open, high, low, close } = ohlcData[0];
            const currentDate = new Date();
            const date = formatDate(currentDate);
      
            // Prepare data for batch insert
            insertData.push([date, open, high, low, close, currency_pair]);
          }
        } catch (err) {
          console.error(`Error processing currency pair ${currency_pair}:`, err);
        }
      });

      // Wait for all forex pairs to be processed
      await Promise.all(promises);

      // If there's data to insert, perform bulk insertion
      if (insertData.length > 0) {
        await connection.query(`
          INSERT INTO history_chart (Date, Open, High, Low, Close, Symbol)
          VALUES ?
        `, [insertData]);

        console.log(`Inserted ${insertData.length} 1-min candlestick records.`);
      }

    } catch (err) {
      console.error('Error processing candlestick data:', err);
    } finally {
      // Release connection in the finally block to ensure it's released even if an error occurs
      if (connection) connection.release();
    }
  });
};

// Utility function to format the date in 'YYYY-MM-DD HH:mm:ss.SSS' format
const formatDate = (date) => {
  return date.getUTCFullYear() + '-' +
    String(date.getUTCMonth() + 1).padStart(2, '0') + '-' +
    String(date.getUTCDate()).padStart(2, '0') + ' ' +
    String(date.getUTCHours()).padStart(2, '0') + ':' +
    String(date.getUTCMinutes()).padStart(2, '0') + ':' +
    String(date.getUTCSeconds()).padStart(2, '0') + '.' +
    String(date.getUTCMilliseconds()).padStart(3, '0');
};
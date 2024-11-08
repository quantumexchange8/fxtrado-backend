import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  let symbolGroupMap = {};

  // Function to fetch and update symbolGroupMap every second
  const updateSymbolGroupMap = async () => {
    let connection;
    try {
      connection = await fastify.mysql.getConnection();
      const [groupSymbols] = await connection.query(
        'SELECT symbol, group_name, spread FROM group_symbols WHERE status = "active"'
      );

      symbolGroupMap = groupSymbols.reduce((acc, { symbol, group_name, spread }) => {
        acc[symbol] = acc[symbol] || [];
        acc[symbol].push({ group_name, spread });
        return acc;
      }, {});
    } catch (err) {
      console.error("Error updating symbol group map:", err);
    } finally {
      if (connection) connection.release();
    }
  };
  
  // Start the interval to update the symbolGroupMap every second
  setInterval(updateSymbolGroupMap, 1000);

  // Run every minute using cron
  cron.schedule('* * * * *', async () => {
    let connection;
    try {
      connection = await fastify.mysql.getConnection();

      const [forexPairs] = await connection.query(
        'SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"'
      );

      const currentDate = new Date();
      currentDate.setUTCSeconds(0, 0); // Reset seconds and milliseconds to zero
      currentDate.setUTCMinutes(currentDate.getUTCMinutes() - 1);
      const date = formatDate(currentDate);

      const insertData = [];
      for (const { symbol_pair, digits } of forexPairs) {
        const [tickData] = await connection.query(
          `SELECT Bid, Ask FROM ticks 
           WHERE Symbol = ? 
           ORDER BY Date DESC LIMIT 1`,
          [symbol_pair]
        );

        if (tickData.length) {
          const { Bid: openPrice, Ask: tempClosePrice } = tickData[0];
          const symbolGroups = symbolGroupMap[symbol_pair] || [];

          symbolGroups.forEach(({ group_name, spread }) => {
            let spreadFactor;

            if (digits === 5) {
              spreadFactor = spread / Math.pow(10, digits);  // e.g., EURUSD or similar pairs
            } else if (digits === 3) {
              spreadFactor = spread / 1000;  // e.g., USDJPY or similar pairs
            } else if (digits === 2) {
              spreadFactor = spread / 100;   // For assets with two decimal places
            } else if (digits === 1) {
              spreadFactor = spread / 10;    // For assets with one decimal place
            } else {
              spreadFactor = spread / Math.pow(10, digits);  // Default fallback for other cases
            }

            const adjustedOpen = openPrice + spreadFactor;
            const adjustedClose = tempClosePrice + spreadFactor;

            insertData.push([
              group_name,
              date,
              currentDate,
              adjustedOpen, // Open
              adjustedOpen, // High
              adjustedOpen, // Low
              adjustedClose, // Close
              symbol_pair
            ]);
          });
        }
      }

      if (insertData.length) {
        await connection.beginTransaction();
        await connection.query(
          `INSERT INTO history_charts (\`group\`, Date, local_date, Open, High, Low, Close, Symbol)
           VALUES ?`,
          [insertData]
        );
        await connection.commit();
        console.log(`Inserted initial OHLC records for ${insertData.length} records.`);
      }

      // Update High, Low, and Close values every second during the minute
      const updateInterval = setInterval(async () => {
        try {
          const [latestTicks] = await connection.query(
            `SELECT Symbol, Bid, Ask, digits FROM ticks 
             WHERE Date >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)`
          );

          latestTicks.forEach(({ Symbol, Bid, Ask, digits }) => {
            const symbolGroups = symbolGroupMap[Symbol] || [];
            const highPrice = Math.max(Bid, Ask);
            const lowPrice = Math.min(Bid, Ask);

            symbolGroups.forEach(async ({ group_name, spread }) => {
               let spreadFactor;
            
              if (digits === 5) {
                spreadFactor = spread / Math.pow(10, digits);  // e.g., EURUSD or similar pairs
              } else if (digits === 3) {
                spreadFactor = spread / 1000;  // e.g., USDJPY or similar pairs
              } else if (digits === 2) {
                spreadFactor = spread / 100;   // For assets with two decimal places
              } else if (digits === 1) {
                spreadFactor = spread / 10;    // For assets with one decimal place
              } else {
                spreadFactor = spread / Math.pow(10, digits);  // Default fallback for other cases
              }

              await connection.query(
                `UPDATE history_charts 
                 SET High = GREATEST(High, ?), Low = LEAST(Low, ?)
                 WHERE Symbol = ? AND Date = ? AND \`group\` = ?`,
                [highPrice + spreadFactor, lowPrice + spreadFactor, Symbol, date, group_name]
              );
            });
          });
        } catch (updateError) {
          console.error("Error updating high/low prices:", updateError);
        }
      }, 1000);

      // Schedule the Close update at 59 seconds
      setTimeout(async () => {
        clearInterval(updateInterval);
        const [closeTickData] = await connection.query(
          `SELECT Symbol, Bid, digits FROM ticks 
           WHERE Date >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)`
        );

        for (const { Symbol, Bid, digits } of closeTickData) {
          const symbolGroups = symbolGroupMap[Symbol] || [];
          for (const { group_name, spread } of symbolGroups) {
            let spreadFactor;
            
            if (digits === 5) {
              spreadFactor = spread / Math.pow(10, digits);  // e.g., EURUSD or similar pairs
            } else if (digits === 3) {
              spreadFactor = spread / 1000;  // e.g., USDJPY or similar pairs
            } else if (digits === 2) {
              spreadFactor = spread / 100;   // For assets with two decimal places
            } else if (digits === 1) {
              spreadFactor = spread / 10;    // For assets with one decimal place
            } else {
              spreadFactor = spread / Math.pow(10, digits);  // Default fallback for other cases
            }

            await connection.query(
              `UPDATE history_charts 
               SET Close = ?
               WHERE Symbol = ? AND Date = ? AND \`group\` = ?`,
              [Bid + spreadFactor, Symbol, date, group_name]
            );
          }
        }
      }, 59000); // Close update at 59 seconds

    } catch (err) {
      console.error("Error processing candlestick data:", err);
      if (connection) await connection.rollback();
    } finally {
      if (connection) connection.release();
    }
  });
};

// Utility function to format the date in 'YYYY-MM-DD HH:mm:ss.000' format
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
    '.000'
  );
};

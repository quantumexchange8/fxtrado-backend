import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  // Initialize the symbol group map outside of intervals to be shared by both intervals
  let symbolGroupMap = {};

  // Function to fetch and update symbolGroupMap every second
  const updateSymbolGroupMap = async () => {
    let connection;
    try {
      connection = await fastify.mysql.getConnection();
      const [groupSymbols] = await connection.query(
        'SELECT symbol, group_name, spread FROM group_symbols WHERE status = "active"'
      );

      const newSymbolGroupMap = {};
      groupSymbols.forEach(({ symbol, group_name, spread }) => {
        if (!newSymbolGroupMap[symbol]) {
          newSymbolGroupMap[symbol] = [];
        }
        newSymbolGroupMap[symbol].push({ symbol, group_name, spread });
      });
      symbolGroupMap = newSymbolGroupMap; // Update the map with latest data
    } catch (err) {
      console.error("Error updating symbol group map:", err);
    } finally {
      if (connection) connection.release();
    }
  };
  
  // Start the interval to update the symbolGroupMap every second
  setInterval(updateSymbolGroupMap, 1000);

  // Run every minute
  setInterval(async () => {
    let connection;
    try {
      connection = await fastify.mysql.getConnection();

      // Fetch all active forex pairs
      const [forexPairs] = await connection.query(
        'SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"'
      );

      const currentDate = new Date();
      currentDate.setUTCSeconds(0, 0); // Reset seconds and milliseconds to zero
      const date = formatDate(currentDate);

      // Insert initial OHLC data for all symbols at the start of the minute
      const insertData = [];
      const promises = forexPairs.map(async ({ symbol_pair, digits }) => {
        const [tickData] = await connection.query(
          `SELECT Bid, Ask, digits FROM ticks 
           WHERE Symbol = ? 
           ORDER BY Date DESC LIMIT 1`,
          [symbol_pair]
        );

        if (tickData && tickData.length) {
          const { Bid: openPrice } = tickData[0];
          if (symbolGroupMap[symbol_pair]) {
            for (const { group_name, spread } of symbolGroupMap[symbol_pair]) {
              const spreadFactor = spread / Math.pow(10, digits);
              const adjustedOpen = openPrice + spreadFactor;

              insertData.push([
                group_name,
                date,
                currentDate,
                adjustedOpen, // Open
                adjustedOpen, // High initially set to open
                adjustedOpen, // Low initially set to open
                adjustedOpen, // Close is initially null
                symbol_pair
              ]);
            }
          }
        }
      });

      await Promise.all(promises);

      // Perform bulk insertion for initial OHLC data
      if (insertData.length > 0) {
        await connection.beginTransaction();
        await connection.query(
          `INSERT INTO history_charts (\`group\`, Date, local_date, Open, High, Low, Close, Symbol)
           VALUES ?`,
          [insertData]
        );
        await connection.commit();
        console.log(`Inserted initial OHLC records for ${insertData.length} records.`);
      }

      // Update High and Low values during the minute
      setInterval(async () => {
        try {
          for (const { symbol_pair, digits } of forexPairs) {
            if (symbolGroupMap[symbol_pair]) {
              const [latestTick] = await connection.query(
                `SELECT Bid, Ask FROM ticks 
                 WHERE Symbol = ? 
                 AND Date >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)`,
                [symbol_pair]
              );

              if (latestTick && latestTick.length) {
                const { Bid, Ask } = latestTick[0];
                const highPrice = Math.max(Bid, Ask);
                const lowPrice = Math.min(Bid, Ask);

                for (const { group_name, spread } of symbolGroupMap[symbol_pair]) {
                  const spreadFactor = spread / Math.pow(10, digits);
                  // console.log('spreadFactor', spreadFactor)

                  await connection.query(
                    `UPDATE history_charts 
                     SET High = GREATEST(High, ?), Low = LEAST(Low, ?)
                     WHERE Symbol = ? AND Date = ? AND \`group\` = ?`,
                    [highPrice + spreadFactor, lowPrice + spreadFactor, symbol_pair, date, group_name]
                  );
                }
              }
            }
          }
        } catch (updateError) {
          console.error("Error updating high/low prices:", updateError);
        }
      }, 1000); // Check every second for updates

      // Update the Close price at the end of the minute
      cron.schedule('59 * * * * *', async () => {
        const [closeTickData] = await connection.query(
          `SELECT Symbol, Bid, digits FROM ticks 
           WHERE Date >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)`
        );

        closeTickData.forEach(async ({ Symbol, Bid, digits }) => {
          if (symbolGroupMap[Symbol]) {
            for (const { group_name, spread } of symbolGroupMap[Symbol]) {
              const spreadFactor = spread / Math.pow(10, digits);

              const closeBid = Bid + spreadFactor;
              // console.log('closeBid', closeBid, '= ', Bid, spreadFactor)
              await connection.query(
                `UPDATE history_charts 
                 SET Close = ?
                 WHERE Symbol = ? AND Date = ? AND \`group\` = ?`,
                [closeBid, Symbol, date, group_name]
              );
            }
          }
        });
      }); // Update close price at the end of the minute (59 seconds in)

    } catch (err) {
      console.error("Error processing candlestick data:", err);
      if (connection) await connection.rollback();
    } finally {
      if (connection) connection.release();
    }
  }, 60000);
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

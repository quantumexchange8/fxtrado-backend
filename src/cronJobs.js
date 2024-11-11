import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  let symbolGroupMap = {};

  const getConnection = async () => {
    try {
      return await fastify.mysql.getConnection();
    } catch (err) {
      console.error("Error obtaining database connection:", err);
    }
  };

  // Helper function to calculate spread factor based on digits
  const calculateSpreadFactor = (spread, digits) => {
    switch (digits) {
      case 5: return spread / 100000;
      case 3: return spread / 1000;
      case 2: return spread / 100;
      case 1: return spread / 10;
      default: return spread / Math.pow(10, digits);
    }
  };

  // Update symbolGroupMap every second
  const updateSymbolGroupMap = async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
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
      connection.release();
    }
  };

  setInterval(updateSymbolGroupMap, 1000);

  // Format date as 'YYYY-MM-DD HH:mm:ss.000'
  const formatDate = (date) => {
    return (
      date.getUTCFullYear() +
      '-' + String(date.getUTCMonth() + 1).padStart(2, '0') +
      '-' + String(date.getUTCDate()).padStart(2, '0') +
      ' ' + String(date.getUTCHours()).padStart(2, '0') +
      ':' + String(date.getUTCMinutes()).padStart(2, '0') +
      ':' + String(date.getUTCSeconds()).padStart(2, '0') + '.000'
    );
  };

  let updateInterval = null; // Global variable to store the interval
  let currentMinute; // Global variable to store the current minute

  const runUpdateHighLow = async (connection, date) => {

    return new Promise((resolve) => {
      const updateHighLow = async () => {
        const newDate = new Date();
        const newMinute = newDate.getUTCMinutes();
  
        // Stop updating if the minute has changed
        if (newMinute !== currentMinute) {
          clearInterval(updateInterval);
          updateInterval = null;
          console.log(`Stopped updates for minute: ${currentMinute}`);
          resolve(); // Resolve promise to indicate completion of updates
          return;
        }
  
        const [latestTicks] = await connection.query(
          `SELECT Symbol, Bid, Ask, digits FROM ticks 
           WHERE Date >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL SECOND(UTC_TIMESTAMP()) SECOND)`
        );
  
        for (const { Symbol, Bid, Ask, digits } of latestTicks) {
          const symbolGroups = symbolGroupMap[Symbol] || [];
          const highPrice = Math.max(Bid, Ask);
          const lowPrice = Math.min(Bid, Ask);
          const closePrice = Bid;
  
          for (const { group_name, spread } of symbolGroups) {
            const spreadFactor = calculateSpreadFactor(spread, digits);
            const roundedHigh = parseFloat((highPrice + spreadFactor).toFixed(digits));
            const roundedLow = parseFloat((lowPrice + spreadFactor).toFixed(digits));
            const roundedClose = parseFloat((closePrice + spreadFactor).toFixed(digits));

            await connection.query(
              'UPDATE history_charts SET High = GREATEST(High, ?), Low = LEAST(Low, ?), Close = ? WHERE Symbol = ? AND Date = ? AND `group` = ?',
              [roundedHigh, roundedLow, roundedClose, Symbol, date, group_name]
            );
          }
        }
      };
  
      updateInterval = setInterval(updateHighLow, 1000);
    });
  };

  cron.schedule('* * * * *', async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
      const [forexPairs] = await connection.query(
        'SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"'
      );

      // Step 1: Set the current minute timestamp (rounded to the nearest minute)
      const currentDate = new Date();
      currentDate.setUTCSeconds(0, 0);
      const date = formatDate(currentDate);

      // Clear the previous interval if it exists, preventing overlap
      if (updateInterval) clearInterval(updateInterval);
      currentMinute = currentDate.getUTCMinutes(); // Set the current minute to track

      // Fetch existing records to avoid duplicates
      const symbolList = forexPairs.map(pair => pair.symbol_pair);
      const [existingRecords] = await connection.query(
        'SELECT Symbol, Date, `group` FROM history_charts WHERE Date = ? AND Symbol IN (?)',
        [date, symbolList]
      );

      const existingKeys = new Set(existingRecords.map(record => `${record.Symbol}-${record.Date}-${record.group}`));
      const insertData = [];

      // Step 2: Insert initial OHLC values for the new minute
      for (const { symbol_pair, digits } of forexPairs) {
        const [tickData] = await connection.query(
          'SELECT Bid, Ask FROM ticks WHERE Symbol = ? ORDER BY Date DESC LIMIT 1',
          [symbol_pair]
        );

        if (tickData.length) {
          const { Bid: openPrice, Ask: tempClosePrice } = tickData[0];
          const symbolGroups = symbolGroupMap[symbol_pair] || [];

          symbolGroups.forEach(({ group_name, spread }) => {
            const spreadFactor = calculateSpreadFactor(spread, digits);
            const adjustedOpen = (openPrice + spreadFactor).toFixed(digits);
            const adjustedClose = (tempClosePrice + spreadFactor).toFixed(digits);

            if (!existingKeys.has(`${symbol_pair}-${date}-${group_name}`)) {
              insertData.push([
                group_name, date, currentDate, adjustedOpen, adjustedOpen, adjustedOpen, adjustedClose, symbol_pair
              ]);
            }
          });
        }
      }

      if (insertData.length) {
        await connection.beginTransaction();
        await connection.query(
          'INSERT INTO history_charts (`group`, Date, local_date, Open, High, Low, Close, Symbol) VALUES ?',
          [insertData]
        );
        await connection.commit();
        console.log(`Inserted initial OHLC records for ${insertData.length} records.`);
      }

      // Await completion of the `updateHighLow` updates for this minute
      await runUpdateHighLow(connection, date);

    } catch (err) {
      console.error("Error processing candlestick data:", err);
      if (connection) await connection.rollback();
    } finally {
      connection.release();
    }
  });
};

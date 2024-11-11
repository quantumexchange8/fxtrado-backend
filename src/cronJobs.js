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
  const calculateSpreadFactor = (spread, digits) => spread / Math.pow(10, digits);

  // Update symbolGroupMap every second
  const updateSymbolGroupMap = async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
      const [groupSymbols] = await connection.query(
        'SELECT symbol, group_name, spread FROM group_symbols WHERE status = "active"'
      );
      symbolGroupMap = groupSymbols.reduce((acc, { symbol, group_name, spread }) => {
        if (!acc[symbol]) acc[symbol] = [];
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
    return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')} ${String(date.getUTCHours()).padStart(2, '0')}:${String(date.getUTCMinutes()).padStart(2, '0')}:${String(date.getUTCSeconds()).padStart(2, '0')}.000`;
  };

  // Function to fetch ticks for the given symbol list within the current minute range
  const getCurrentMinPrice = async (connection, symbolList, currentMinuteStart) => {
    const currentTime = new Date();
    const formattedStart = currentMinuteStart.toISOString().slice(0, 19).replace('T', ' ');
    const formattedEnd = currentTime.toISOString().slice(0, 19).replace('T', ' ');

    try {
      const [latestTicks] = await connection.query(
        `SELECT Symbol, Bid, Ask, digits FROM ticks WHERE Symbol IN (?) AND Date BETWEEN ? AND ? ORDER BY Date DESC`,
        [symbolList, formattedStart, formattedEnd]
      );
      return latestTicks;
    } catch (err) {
      console.error("Error fetching tick data:", err);
      return [];
    }
  };

  const runUpdateHighLow = async (connection, date, symbolList) => {
    const currentMinute = new Date().getUTCMinutes();
    const currentMinuteStart = new Date();
    currentMinuteStart.setUTCSeconds(0, 0);

    const updateInterval = setInterval(async () => {
      const newDate = new Date();
      const newMinute = newDate.getUTCMinutes();
  
      if (newMinute !== currentMinute) {
        clearInterval(updateInterval); // Stop interval
        console.log(`Stopped updates for minute: ${currentMinute}`);
        return;
      }

      const latestTicks = await getCurrentMinPrice(connection, symbolList, currentMinuteStart);

      if (latestTicks.length === 0) {
        console.log("No tick data found for the current minute.");
        return;
      }

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
    }, 1000); // Update every second
  };

  cron.schedule('* * * * *', async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
      const [forexPairs] = await connection.query('SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"');
      const currentDate = new Date();
      currentDate.setUTCSeconds(0, 0);
      const date = formatDate(currentDate);

      const symbolList = forexPairs.map(pair => pair.symbol_pair);
      const [existingRecords] = await connection.query(
        'SELECT Symbol, Date, `group` FROM history_charts WHERE Date = ? AND Symbol IN (?)',
        [date, symbolList]
      );

      const existingKeys = new Set(existingRecords.map(record => `${record.Symbol}-${record.Date}-${record.group}`));
      const insertData = [];

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
              insertData.push([group_name, date, currentDate, adjustedOpen, adjustedOpen, adjustedOpen, adjustedClose, symbol_pair]);
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

      await runUpdateHighLow(connection, date, symbolList);

    } catch (err) {
      console.error("Error processing candlestick data:", err);
      if (connection) await connection.rollback();
    } finally {
      connection.release();
    }
  });
};

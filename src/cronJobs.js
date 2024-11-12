import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  let symbolGroupMap = {};
  let isUpdatingHighLow = false;

  // Establish and manage MySQL connection
  const getConnection = async () => {
    try {
      return await fastify.mysql.getConnection();
    } catch (err) {
      console.error("Error obtaining database connection:", err);
    }
  };

  // Helper to calculate spread factor based on digits
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
    return date.toISOString().slice(0, 19).replace('T', ' ');
  };

  // Fetch ticks for a given symbol list within the current minute range
  const getCurrentMinPrice = async (connection, symbolList, currentMinuteStart) => {
    const formattedStart = formatDate(currentMinuteStart);
    const formattedEnd = formatDate(new Date());

    try {
      const [latestTicks] = await connection.query(
        `SELECT Date, Symbol, Bid, Ask, digits FROM ticks WHERE Symbol IN (?) AND Date BETWEEN ? AND ? ORDER BY Date DESC`,
        [symbolList, formattedStart, formattedEnd]
      );

      // console.log('latestTicks', latestTicks)

      return latestTicks;
    } catch (err) {
      console.error("Error fetching tick data:", err);
      return [];
    }
  };

  // Run high-low price updates
  const runUpdateHighLow = async (connection, date, symbolList) => {
    if (isUpdatingHighLow) return;

    isUpdatingHighLow = true;
    const currentMinuteStart = new Date();
    currentMinuteStart.setUTCSeconds(0, 0);

    const updateInterval = setInterval(async () => {
      const newMinute = new Date().getUTCMinutes();

      if (newMinute !== currentMinuteStart.getUTCMinutes()) {
        clearInterval(updateInterval);
        isUpdatingHighLow = false;
        console.log(`Stopped updates for minute: ${currentMinuteStart.getUTCMinutes()}`);
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

        const symbolTicks = latestTicks.filter(tick => tick.Symbol === Symbol);

        // Sort by Date (in descending order) to get the last tick of the minute
        const lastTick = symbolTicks.sort((a, b) => new Date(b.Date) - new Date(a.Date))[0];

        // Use the bid from the last tick of the minute
        const closingBid = lastTick ? lastTick.Bid : Bid;

        for (const { group_name, spread } of symbolGroups) {
          const spreadFactor = calculateSpreadFactor(spread, digits);
          const roundedHigh = parseFloat((highPrice + spreadFactor).toFixed(digits));
          const roundedLow = parseFloat((lowPrice + spreadFactor).toFixed(digits));
          const roundedClose = parseFloat((closingBid + spreadFactor).toFixed(digits));

          await connection.query(
            'UPDATE history_charts SET High = GREATEST(High, ?), Low = LEAST(Low, ?), Close = ? WHERE Symbol = ? AND Date = ? AND `group` = ?',
            [roundedHigh, roundedLow, roundedClose, Symbol, date, group_name]
          );
        }
      }
    }, 3000); // Update every second
  };

  // Main cron task to update candlestick data
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

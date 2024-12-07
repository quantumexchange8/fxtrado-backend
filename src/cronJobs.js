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
  const formatDate = (date) => date.toISOString().slice(0, 19).replace('T', ' ');

  // Fetch ticks for a given symbol list within the current minute range
  const getCurrentMinPrice = async (connection, symbolList, currentMinuteStart) => {
    try {
      const now = new Date();
      const nowMinute = now.getUTCMinutes();
      const currentMinuteStartMinute = currentMinuteStart.getUTCMinutes();
  
      // Only fetch the data for the current minute if the minute is the same
      if (nowMinute === currentMinuteStartMinute) {
        const [latestTicks] = await connection.query(
          `SELECT Date, Symbol, Bid, Ask, digits FROM ticks WHERE Symbol IN (?) AND Date BETWEEN ? AND ? ORDER BY Date DESC`,
          [symbolList, formatDate(currentMinuteStart), formatDate(now)]
        );
  
        // console.log('Fetching data for the current minute:', formatDate(currentMinuteStart), formatDate(now));
        return latestTicks;
      }
    } catch (err) {
      console.error("Error fetching tick data:", err);
      return [];
    }
  };

  // Run high-low price updates
  const runUpdateHighLow = async (connection, date, symbolList) => {
    if (isUpdatingHighLow) return;
    isUpdatingHighLow = true;
  
    let currentMinuteStart = new Date();
    currentMinuteStart.setUTCSeconds(0, 0);
  
    const updateInterval = setInterval(async () => {
      const now = new Date();
      if (now.getUTCMinutes() !== currentMinuteStart.getUTCMinutes()) {
        clearInterval(updateInterval);
        isUpdatingHighLow = false;
        console.log(`Stopped updates for minute: ${currentMinuteStart.getUTCMinutes()}`);

        currentMinuteStart = new Date();
        currentMinuteStart.setUTCSeconds(0, 0); 
        return;
      }
  
      const latestTicks = await getCurrentMinPrice(connection, symbolList, currentMinuteStart);
      if (!latestTicks.length) {
        console.log("No tick data found for the current minute.");
        return;
      }
  
      // Accumulate updates for a batch query
      const casesHigh = [];
      const casesLow = [];
      const casesClose = [];
      const symbolsGroups = new Set();
  
      latestTicks.forEach(({ Symbol, Bid, Ask, digits }) => {
        const symbolGroups = symbolGroupMap[Symbol] || [];
        const highPrice = Math.max(Bid, Ask);
        const lowPrice = Math.min(Bid, Ask);
        const closingBid = latestTicks.find(tick => tick.Symbol === Symbol)?.Bid || Bid;

        symbolGroups.forEach(({ group_name, spread }) => {
          const spreadFactor = calculateSpreadFactor(spread, digits);
          const roundedHigh = parseFloat((highPrice + spreadFactor).toFixed(digits));
          const roundedLow = parseFloat((lowPrice + spreadFactor).toFixed(digits));
          const roundedClose = parseFloat((closingBid + spreadFactor).toFixed(digits));
          const formattedDate = formatDate(currentMinuteStart);

          casesHigh.push(`WHEN Symbol = '${Symbol}' AND Date = '${formattedDate}' AND \`group\` = '${group_name}' THEN GREATEST(High, ${roundedHigh})`);
          casesLow.push(`WHEN Symbol = '${Symbol}' AND Date = '${formattedDate}' AND \`group\` = '${group_name}' THEN LEAST(Low, ${roundedLow})`);
          casesClose.push(`WHEN Symbol = '${Symbol}' AND Date = '${formattedDate}' AND \`group\` = '${group_name}' THEN ${roundedClose}`);
          symbolsGroups.add(`'${Symbol}-${group_name}-${formattedDate}'`);
        });
      });

      const updateQuery = `
        UPDATE history_charts
        SET 
          High = CASE ${casesHigh.join(' ')} END,
          Low = CASE ${casesLow.join(' ')} END,
          Close = CASE ${casesClose.join(' ')} END
        WHERE CONCAT(Symbol, '-', \`group\`, '-', Date) IN (${[...symbolsGroups].join(', ')})
      `;
  
      try {
        await connection.query(updateQuery);
        // console.log("Batch update completed for OHLC data.");
      } catch (err) {
        console.error("Error performing batch update:", err);
      }
  
    }, 2000); // Update every 3 seconds
  };

  // Main cron task to update candlestick data
  cron.schedule('* * * * *', async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
      const [forexPairs] = await connection.query(
        'SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"'
      );
      const date = formatDate(new Date(new Date().setUTCSeconds(0, 0)));
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
            if (!existingKeys.has(`${symbol_pair}-${date}-${group_name}`)) {
              insertData.push([
                group_name,
                date,
                new Date(),
                parseFloat((openPrice + spreadFactor).toFixed(digits)),
                parseFloat((openPrice + spreadFactor).toFixed(digits)),
                parseFloat((openPrice + spreadFactor).toFixed(digits)),
                parseFloat((tempClosePrice + spreadFactor).toFixed(digits)),
                symbol_pair
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

      await runUpdateHighLow(connection, date, symbolList);
    } catch (err) {
      console.error("Error processing candlestick data:", err);
      if (connection) await connection.rollback();
    } finally {
      connection.release();
    }
  });
};

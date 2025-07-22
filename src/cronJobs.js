import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  let symbolGroupMap = {};
  let currentMinute = null;

  // Establish and manage MySQL connection
  const getConnection = async (retries = 5, delay = 1000) => {
  while (retries > 0) {
    try {
      return await fastify.mysql.getConnection();
    } catch (err) {
      console.error("Error obtaining database connection:", err);
      if (--retries > 0) await new Promise(res => setTimeout(res, delay));
    }
  }
  return null;
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

  // Fetch the latest bid/ask price for each symbol
  const fetchLatestTicks = async (connection, symbolList) => {
    try {
      const [ticks] = await connection.query(
        `SELECT Symbol, Bid, Ask, digits FROM ticks WHERE Symbol IN (?) AND Date = (SELECT MAX(Date) FROM ticks WHERE symbol = ticks.symbol)`,
        [symbolList]
      );
      return ticks.reduce((acc, { Symbol, Bid, Ask, digits }) => {
        acc[Symbol] = { Bid, Ask, digits };
        return acc;
      }, {});
    } catch (err) {
      console.error("Error fetching latest tick data:", err);
      return {};
    }
  };

  // Insert new row at the start of each minute
  const insertNewMinuteRows = async (connection, date) => {

    try {

      const [forexPairs] = await connection.query('SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"');

      const symbolList = forexPairs.map(pair => pair.symbol_pair);

      const latestTicks = await fetchLatestTicks(connection, symbolList);


      const insertData = [];
      const currentTime = new Date();

      for (const { symbol_pair, digits } of forexPairs) {
        const tick = latestTicks[symbol_pair];
        if (!tick) continue;
  
        const { Bid, Ask } = tick;
        const symbolGroups = symbolGroupMap[symbol_pair] || [];
  
        for (const { group_name, spread } of symbolGroups) {
          const spreadFactor = calculateSpreadFactor(spread, digits);
          const adjustedBid = parseFloat((Bid + spreadFactor).toFixed(digits));
          const adjustedAsk = parseFloat((Ask + spreadFactor).toFixed(digits));
  
          // Check if a record already exists
          const [existingRow] = await connection.query(
            'SELECT 1 FROM history_charts WHERE `group` = ? AND `Date` = ? AND `Symbol` = ? LIMIT 1',
            [group_name, date, symbol_pair]
          );
  
          if (!existingRow.length) {
            // Prepare data for batch insert
            insertData.push([
              group_name,    // group
              date,          // Date (UTC minute)
              currentTime,    // local_date (server time)
              adjustedBid,   // Open price
              adjustedBid,   // High price
              adjustedBid,   // Low price
              adjustedAsk,   // Close price
              symbol_pair    // Symbol
            ]);
          }
        }
      }
  
      // Execute the batch insert if data exists
      if (insertData.length) {
        await connection.query(
          'INSERT INTO history_charts (`group`, Date, local_date, Open, High, Low, Close, Symbol) VALUES ?',
          [insertData]
        );
      }
    } catch (err) {
      console.error("Error inserting new minute rows:", err);
    }
  };


  // Update the latest row with high, low, close prices
  const updateCurrentMinuteRows = async (connection, symbolList) => {
    const now = new Date();
    now.setUTCSeconds(0, 0);
    const date = formatDate(now);
    const latestTicks = await fetchLatestTicks(connection, symbolList);

    const casesHigh = [];
    const casesLow = [];
    const casesClose = [];
    const symbolsGroups = new Set();

    for (const [Symbol, { Bid, Ask, digits }] of Object.entries(latestTicks)) {
      const highPrice = Math.max(Bid, Ask);
      const lowPrice = Math.min(Bid, Ask);
      const closingBid = Bid;
      const symbolGroups = symbolGroupMap[Symbol] || [];

      for (const { group_name, spread } of symbolGroups) {
        const spreadFactor = calculateSpreadFactor(spread, digits);
        const roundedHigh = parseFloat((highPrice + spreadFactor).toFixed(digits));
        const roundedLow = parseFloat((lowPrice + spreadFactor).toFixed(digits));
        const roundedClose = parseFloat((closingBid + spreadFactor).toFixed(digits));

        casesHigh.push(`WHEN Symbol = '${Symbol}' AND Date = '${date}' AND \`group\` = '${group_name}' THEN GREATEST(High, ${roundedHigh})`);
        casesLow.push(`WHEN Symbol = '${Symbol}' AND Date = '${date}' AND \`group\` = '${group_name}' THEN LEAST(Low, ${roundedLow})`);
        casesClose.push(`WHEN Symbol = '${Symbol}' AND Date = '${date}' AND \`group\` = '${group_name}' THEN ${roundedClose}`);
        symbolsGroups.add(`'${Symbol}-${group_name}-${date}'`);
      }
    }

    if (casesHigh.length) {
      const updateQuery = `
        UPDATE history_charts
        SET 
          High = CASE ${casesHigh.join(' ')} END,
          Low = CASE ${casesLow.join(' ')} END,
          Close = CASE ${casesClose.join(' ')} END
        WHERE CONCAT(Symbol, '-', \`group\`, '-', Date) IN (${[...symbolsGroups].join(', ')});
      `;
      await connection.query(updateQuery);
    }
  };

  // Cron for inserting new rows
  cron.schedule('* * * * *', async () => {
    
    const connection = await getConnection();
    if (!connection) return;

    try {
      const date = formatDate(new Date(new Date().setUTCSeconds(0, 0)));

      // Insert new rows for the current minute
      await insertNewMinuteRows(connection, date);
    } catch (err) {
      console.error("Error in cron task:", err);
    } finally {
      connection.release();
    }
  });

  // Cron for updating current minute rows
  cron.schedule('* * * * * *', async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
      const [forexPairs] = await connection.query(
        'SELECT symbol_pair FROM forex_pairs WHERE status = "active"'
      );
      const symbolList = forexPairs.map(pair => pair.symbol_pair);

      // Update rows for the current minute
      await updateCurrentMinuteRows(connection, symbolList);
    } catch (err) {
      console.error("Error in update cron task:", err);
    } finally {
      connection.release();
    }
  });
};

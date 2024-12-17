import cron from 'node-cron';

export const startVolumeCreation = (fastify) => {
  let symbolGroupMap = {};
  let currentMinute = null;

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
  const insertNewMinuteRows = async (connection, symbolList, date) => {

    console.log('now date', new Date())
    const [forexPairs] = await connection.query(
      'SELECT symbol_pair, digits FROM forex_pairs WHERE status = "active"'
    );
    const latestTicks = await fetchLatestTicks(connection, symbolList);

    const insertData = [];

    forexPairs.forEach(({ symbol_pair, digits }) => {
      const tick = latestTicks[symbol_pair];
      if (tick) {
        const { Bid, Ask } = tick;
        const symbolGroups = symbolGroupMap[symbol_pair] || [];

        symbolGroups.forEach(({ group_name, spread }) => {
          const spreadFactor = calculateSpreadFactor(spread, digits);
          insertData.push([
            group_name,
            date,
            new Date(),
            parseFloat((Bid + spreadFactor).toFixed(digits)),
            parseFloat((Bid + spreadFactor).toFixed(digits)),
            parseFloat((Bid + spreadFactor).toFixed(digits)),
            parseFloat((Ask + spreadFactor).toFixed(digits)),
            symbol_pair
          ]);
        });
      }
    });

    if (insertData.length) {
      await connection.query(
        'INSERT INTO history_charts (`group`, Date, local_date, Open, High, Low, Close, Symbol) VALUES ?',
        [insertData]
      );
    }
  };

  // Update the latest row with high, low, close prices
  const updateCurrentMinuteRows = async (connection, symbolList, date) => {
    const latestTicks = await fetchLatestTicks(connection, symbolList);
    const casesHigh = [];
    const casesLow = [];
    const casesClose = [];
    const symbolsGroups = new Set();

    Object.entries(latestTicks).forEach(([Symbol, { Bid, Ask, digits }]) => {
      const highPrice = Math.max(Bid, Ask);
      const lowPrice = Math.min(Bid, Ask);
      const closingBid = Bid;
      const symbolGroups = symbolGroupMap[Symbol] || [];

      symbolGroups.forEach(({ group_name, spread }) => {
        const spreadFactor = calculateSpreadFactor(spread, digits);
        const roundedHigh = parseFloat((highPrice + spreadFactor).toFixed(digits));
        const roundedLow = parseFloat((lowPrice + spreadFactor).toFixed(digits));
        const roundedClose = parseFloat((closingBid + spreadFactor).toFixed(digits));

        casesHigh.push(`WHEN Symbol = '${Symbol}' AND Date = '${date}' AND \`group\` = '${group_name}' THEN GREATEST(High, ${roundedHigh})`);
        casesLow.push(`WHEN Symbol = '${Symbol}' AND Date = '${date}' AND \`group\` = '${group_name}' THEN LEAST(Low, ${roundedLow})`);
        casesClose.push(`WHEN Symbol = '${Symbol}' AND Date = '${date}' AND \`group\` = '${group_name}' THEN ${roundedClose}`);
        symbolsGroups.add(`'${Symbol}-${group_name}-${date}'`);
      });
    });

    const updateQuery = `
      UPDATE history_charts
      SET 
        High = CASE ${casesHigh.join(' ')} END,
        Low = CASE ${casesLow.join(' ')} END,
        Close = CASE ${casesClose.join(' ')} END
      WHERE CONCAT(Symbol, '-', \`group\`, '-', Date) IN (${[...symbolsGroups].join(', ')});
    `;

    if (casesHigh.length) {
      await connection.query(updateQuery);
    }
  };

  // Persistent update loop for the current minute
  const startUpdateLoop = (symbolList) => {
    setInterval(async () => {
      const now = new Date();
      now.setUTCSeconds(0, 0);
      const date = formatDate(now);

      if (!currentMinute || currentMinute !== date) {
        currentMinute = date;
        return;
      }

      const connection = await getConnection();
      if (connection) {
        try {
          await updateCurrentMinuteRows(connection, symbolList, date);
        } catch (err) {
          console.error("Error updating current minute rows:", err);
        } finally {
          connection.release();
        }
      }
    }, 1000);
  };

  // Main cron task
  cron.schedule('* * * * *', async () => {
    const connection = await getConnection();
    if (!connection) return;

    try {
      const date = formatDate(new Date(new Date().setUTCSeconds(0, 0)));
      const [forexPairs] = await connection.query(
        'SELECT symbol_pair FROM forex_pairs WHERE status = "active"'
      );
      const symbolList = forexPairs.map(pair => pair.symbol_pair);

      // Insert new rows for the current minute
      await insertNewMinuteRows(connection, symbolList, date);

      // Start the update loop
      startUpdateLoop(symbolList);
    } catch (err) {
      console.error("Error in cron task:", err);
    } finally {
      connection.release();
    }
  });
};

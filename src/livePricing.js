import fastify from 'fastify';
import cron from 'node-cron';

const getAllLatestPrices = async (fastify) => {
  const [forexPairs] = await fastify.mysql.query(
      'SELECT currency_pair, symbol_pair, digits FROM forex_pairs WHERE status = "active"'
  );

  // Get current time truncated to the minute (ignores seconds)
  const currentMinute = new Date();
  const formattedCurrentMinute = currentMinute.toISOString().slice(0, 19).replace('T', ' ');

  const [result] = await fastify.mysql.query(
      `SELECT symbol, bid, ask, Date
       FROM fxtrado.ticks 
       WHERE symbol IN (?) 
       AND Date >= ?`,
      [forexPairs.map(pair => pair.symbol_pair), formattedCurrentMinute]
  );

  return result.map(price => {
      const pair = forexPairs.find(pair => pair.symbol_pair === price.symbol);
      return {
          ...price,
          digits: pair ? pair.digits : null  // Attach the digits from forexPairs
      };
  });
};

let openPrices = {};
const calculateOHLC = (prices) => {
  if (!prices || prices.length === 0) return null;

  openPrices = prices[0].bid;
  // console.log('op', openPrices)
  // console.log('p', prices[0].bid);
  const open = openPrices;  // The first bid price is the open price
  const high = Math.max(...prices.map(price => price.bid)); // The highest bid price
  const low = Math.min(...prices.map(price => price.bid));  // The lowest bid price
  const close = prices[prices.length - 1].bid; // The last bid price is the close price

  return { open, high, low, close };
};

const updateLivePrice = async (fastify) => {
    const latestPrices = await getAllLatestPrices(fastify);
    // console.log('test', latestPrices.length)
    if (latestPrices.length === 0) return [];

    
    // Group prices by symbol
    const pricesBySymbol = latestPrices.reduce((acc, price) => {
      if (!acc[price.symbol]) {
          acc[price.symbol] = [];
      }
      acc[price.symbol].push(price);
      return acc;
  }, {});

    // Construct CASE statements for open, high, low, and close
    let openCase = '';
    let highCase = '';
    let lowCase = '';
    let closeCase = '';
    let symbols = '';

    Object.keys(pricesBySymbol).forEach(symbol => {
      const ohlc = calculateOHLC(pricesBySymbol[symbol]);

      if (ohlc) {
          openCase += `WHEN '${symbol}' THEN ${ohlc.open} `;
          highCase += `WHEN '${symbol}' THEN ${ohlc.high} `;
          lowCase += `WHEN '${symbol}' THEN ${ohlc.low} `;
          closeCase += `WHEN '${symbol}' THEN ${ohlc.close} `;
          symbols += `'${symbol}', `;
      }
    });

    symbols = symbols.slice(0, -2); // Remove trailing comma

    // Update query
    const updates = `
      UPDATE live_ohlc 
      SET 
        open = CASE symbol ${openCase} END,
        high = CASE symbol ${highCase} END,
        low = CASE symbol ${lowCase} END,
        close = CASE symbol ${closeCase} END,
        date = NOW()
      WHERE symbol IN (${symbols});
    `;

    // Execute the query
    await fastify.mysql.query(updates);

    // After every minute, reset the openPrices for the next minute
    setTimeout(() => {
      openPrices = {};  // Clear open prices for the new minute
    }, 60000);  // Reset after one minute
};

// Schedule the cron job to run every second
export const schedeEveryMinOHLC = (fastify) => {
    cron.schedule('* * * * * *', async () => {
        await updateLivePrice(fastify);
    })
};
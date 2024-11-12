import fastify from 'fastify';
import cron from 'node-cron';

// Function to fetch the latest orders and broadcast them
export const updateOpenOrders = async (fastify) => {
  try {
    // Fetch latest open orders from the database
    const prices = await getAllLatestPrices(fastify); // Pass fastify here
    const orders = await getOpenOrders(fastify); // Pass fastify here

    
    // Prepare the data to send
    const data = JSON.stringify({ prices, orders });

  } catch (error) {
    console.error('Error updating open orders:', error);
  }
};

// Function to get group spread
const getAllSpread = async (fastify) => {
  const [spreadData] = await fastify.mysql.query(
    'SELECT group_name, symbol, spread FROM group_symbols WHERE status = "active"'
  );

  const spreadMap = new Map();
  spreadData.forEach(({ group_name, symbol, spread }) => {
    spreadMap.set(`${group_name}_${symbol}`, spread);
  });

  return spreadMap;
};

// Function to get open orders
const getOpenOrders = async (fastify) => {
  const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE status = "open" AND close_price is NULL');

  return orders;
};

// Calculate spread factor based on the number of digits for each symbol
const calculateSpreadFactor = (spread, digits) => {
  switch (digits) {
    case 5:
      return spread / 100000;  // EURUSD or similar with 5 digits
    case 3:
      return spread / 1000;    // USDJPY or similar with 3 digits
    case 2:
      return spread / 100;     // For assets with 2 decimal places
    case 1:
      return spread / 10;      // For assets with 1 decimal place
    default:
      return spread / Math.pow(10, digits);  // Fallback for other cases
  }
};

// Function to get the latest bid and ask prices
const getAllLatestPrices = async (fastify) => {
  const [forexPairs] = await fastify.mysql.query('SELECT currency_pair, symbol_pair, digits FROM forex_pairs WHERE status = "active"');
  
  const [result] = await fastify.mysql.query(
    `SELECT symbol, bid, ask, digits 
     FROM fxtrado.ticks 
     WHERE symbol IN (?) 
     AND Date = (SELECT MAX(Date) FROM fxtrado.ticks WHERE symbol = fxtrado.ticks.symbol)`,
    [forexPairs.map(pair => pair.symbol_pair)]
  );
  return result.map(price => {
    const pair = forexPairs.find(pair => pair.symbol_pair === price.symbol);
    return {
      ...price,
      digits: pair ? pair.digits : null  // Attach the digits from forexPairs
    };
  });
};

const calculatePL = async (fastify) => {
  try {
    // Fetch open orders and latest prices in parallel
    const [orders, latestPrices, spreadMap] = await Promise.all([getOpenOrders(fastify), getAllLatestPrices(fastify), getAllSpread(fastify)]);

    if (orders.length === 0 || latestPrices.length === 0) return []; // No data to process

    // Create a map for fast lookups of latest prices by symbol
    const latestPriceMap = new Map(latestPrices.map(price => [price.symbol, price]));

    // Utility function to calculate profit
    const calculateProfit = (order, latestPrice) => {
      const { type, price, volume, group_name, symbol } = order;

      const openPriceFloat = parseFloat(order.price);
      const lotSizeFloat = parseFloat(order.volume) || 0.01;

      // Retrieve spread for this order's group and symbol
      const spread = spreadMap.get(`${group_name}_${symbol}`) || 0;
      const spreadFactor = calculateSpreadFactor(spread, latestPrice.digits);

      const adjustedBid = parseFloat(latestPrice.bid) + spreadFactor;
      const adjustedAsk = parseFloat(latestPrice.ask) + spreadFactor;

      const multiplier =  latestPrice.digits === 3 ? 1000 : 
                          latestPrice.digits === 5 ? 100000 : 
                          latestPrice.digits === 1 ? 10 : 
                          latestPrice.digits === 2 ? 100 : 1;

      const pipDifference = order.type === 'buy' ? adjustedBid - openPriceFloat : openPriceFloat - adjustedAsk;

      return (pipDifference * lotSizeFloat * multiplier).toFixed(2);
    };

    // Prepare updates
    const updates = [];
    for (const order of orders) {
      const latestPrice = latestPriceMap.get(order.symbol);
      if (!latestPrice) {
        // console.error(`No latest price found for symbol: ${order.symbol}`);
        continue; // Skip this order if no price is found
      }

      const profit = calculateProfit(order, latestPrice);

      // Retrieve spread for this order's group and symbol
      const spread = spreadMap.get(`${order.group_name}_${order.symbol}`) || 0;
      const spreadFactor = calculateSpreadFactor(spread, latestPrice.digits);

      const adjustedBid = parseFloat(latestPrice.bid) + spreadFactor;
      const adjustedAsk = parseFloat(latestPrice.ask) + spreadFactor;

      updates.push({
        id: order.id,
        profit,
        market_bid: adjustedBid.toFixed(latestPrice.digits),
        market_ask: adjustedAsk.toFixed(latestPrice.digits),
      });
    }

    if (updates.length === 0) return []; // No updates, return early

    // Prepare query components for batch update
    const ids = updates.map(u => u.id);
    const profitParams = updates.map(u => u.profit);
    const bidParams = updates.map(u => u.market_bid);
    const askParams = updates.map(u => u.market_ask);

    const updateQuery = `
      UPDATE orders
      SET profit = ?, market_bid = ?, market_ask = ?
      WHERE id = ?
    `;

    const updatePromises = updates.map((update) => fastify.mysql.query(updateQuery, [update.profit, update.market_bid, update.market_ask, update.id]));

    // Execute the batch update query
    await Promise.all(updatePromises);
    
  } catch (error) {
    console.error('Error calculating P/L:', error);
    return [];
  }
};

// Schedule the cron job to run every minute
export const scheduleOpenOrderUpdates = (fastify) => {
    cron.schedule('*/5 * * * * *', async () => {
        await updateOpenOrders(fastify);
        await calculatePL(fastify);
    });
};

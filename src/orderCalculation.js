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

// Function to get open orders
const getOpenOrders = async (fastify) => {
  const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE status = "open" AND close_price is NULL');

  return orders;
};
    
// Function to get the latest bid and ask prices
const getAllLatestPrices = async (fastify) => {
  const [forexPairs] = await fastify.mysql.query('SELECT currency_pair, symbol_pair, digits FROM forex_pairs WHERE status = "active"');
  
  const [result] = await fastify.mysql.query(
    `SELECT symbol, bid, ask 
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
    const [orders, latestPrices] = await Promise.all([getOpenOrders(fastify), getAllLatestPrices(fastify)]);

    if (orders.length === 0 || latestPrices.length === 0) return []; // No data to process

    // Create a map for fast lookups of latest prices by symbol
    const latestPriceMap = new Map(latestPrices.map(price => [price.symbol, price]));

    // Utility function to calculate profit
    const calculateProfit = (order, latestPrice) => {
      const { type, price, volume } = order;
      const currentBid = parseFloat(latestPrice.bid);
      const currentAsk = parseFloat(latestPrice.ask);
      const openPriceFloat = parseFloat(price);
      const lotSizeFloat = parseFloat(volume) || 0.01;
      const digits = latestPrice.digits;
      const multiplier = digits === 3 ? 1000 : digits === 5 ? 100000 : digits === 1 ? 10 : 1;

      let pipDifference = 0;
      if (type === 'buy') {
        pipDifference = currentBid - openPriceFloat;
      } else if (type === 'sell') {
        pipDifference = openPriceFloat - currentAsk;
      }

      return (pipDifference * lotSizeFloat * multiplier).toFixed(2);
    };

    // Prepare updates
    const updates = [];
    for (const order of orders) {
      const latestPrice = latestPriceMap.get(order.symbol);
      if (!latestPrice) {
        console.error(`No latest price found for symbol: ${order.symbol}`);
        continue; // Skip this order if no price is found
      }

      const profit = calculateProfit(order, latestPrice);
      updates.push({
        id: order.id,
        profit,
        market_bid: parseFloat(latestPrice.bid),
        market_ask: parseFloat(latestPrice.ask),
      });
    }

    if (updates.length === 0) return []; // No updates, return early

    // Prepare query components for batch update
    const ids = updates.map(u => u.id).join(', ');
    const profitCases = updates.map(u => `WHEN ${u.id} THEN ${u.profit}`).join(' ');
    const bidCases = updates.map(u => `WHEN ${u.id} THEN ${u.market_bid}`).join(' ');
    const askCases = updates.map(u => `WHEN ${u.id} THEN ${u.market_ask}`).join(' ');

    const updateQuery = `
      UPDATE orders
      SET
        profit = CASE id ${profitCases} END,
        market_bid = CASE id ${bidCases} END,
        market_ask = CASE id ${askCases} END
      WHERE id IN (${ids});
    `;

    // Execute the batch update query
    await fastify.mysql.query(updateQuery);

    return updates; // Return the updated orders
  } catch (error) {
    console.error('Error calculating P/L:', error);
    return [];
  }
};

// Schedule the cron job to run every minute
export const scheduleOpenOrderUpdates = (fastify) => {
    cron.schedule('* * * * * *', async () => {
        await updateOpenOrders(fastify);
        await calculatePL(fastify);
    });
};

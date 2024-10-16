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
  const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE status = "open"');

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
      // Run queries in parallel since they don't depend on each other
      const [orders, latestPrices] = await Promise.all([
          getOpenOrders(fastify),  // Fetch open orders
          getAllLatestPrices(fastify)  // Fetch latest bid/ask prices
      ]);

      // Prepare the updates for batch query
      const updates = orders.map(order => {
          const latestPrice = latestPrices.find(price => price.symbol === order.symbol);

          if (!latestPrice) {
              console.error(`No latest price found for symbol: ${order.symbol}`);
              return null; // Skip this order if no latest price is found
          }

          const { type, price, volume } = order;
          const currentBid = parseFloat(latestPrice.bid);
          const currentAsk = parseFloat(latestPrice.ask);
          const openPriceFloat = parseFloat(price);
          const lotSizeFloat = parseFloat(volume) || 0.01;
          const digits = latestPrice.digits;
          const multiplier = digits === 3 ? 1000 : digits === 5 ? 100000 : digits === 1 ? 10 : 1;

          let pl = 0;
          if (type === 'buy') {
              const pipDifference = currentBid - openPriceFloat;
              pl = pipDifference * lotSizeFloat * multiplier;
          } else if (type === 'sell') {
              const pipDifference = openPriceFloat - currentAsk;
              pl = pipDifference * lotSizeFloat * multiplier;
          }

          return {
              id: order.id,
              profit: pl.toFixed(2),  // Format profit
              market_bid: currentBid,
              market_ask: currentAsk
          };
      }).filter(update => update !== null);  // Filter out null updates

      if (updates.length === 0) return [];  // No updates, return early

      // Construct the batch update query
      const updateQuery = `
          UPDATE orders
          SET profit = CASE id
              ${updates.map(u => `WHEN ${u.id} THEN ${u.profit}`).join(' ')}
          END,
          market_bid = CASE id
              ${updates.map(u => `WHEN ${u.id} THEN ${u.market_bid}`).join(' ')}
          END,
          market_ask = CASE id
              ${updates.map(u => `WHEN ${u.id} THEN ${u.market_ask}`).join(' ')}
          END
          WHERE id IN (${updates.map(u => u.id).join(', ')});
      `;

      // Execute the batch update query
      await fastify.mysql.query(updateQuery);

      return updates;  // Return the updated orders

  } catch (error) {
      console.error('Error calculating P/L:', error);
      return [];
  }
}



// Schedule the cron job to run every minute
export const scheduleOpenOrderUpdates = (fastify) => {
    cron.schedule('* * * * * *', async () => {
        await updateOpenOrders(fastify);
        await calculatePL(fastify);
    });
};

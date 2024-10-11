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
  const [forexPairs] = await fastify.mysql.query('SELECT currency_pair, symbol_pair FROM forex_pairs WHERE status = "active"');
  
  const [result] = await fastify.mysql.query(
    `SELECT symbol, bid, ask 
     FROM fxtrado.ticks 
     WHERE symbol IN (?) 
     AND Date = (SELECT MAX(Date) FROM fxtrado.ticks WHERE symbol = fxtrado.ticks.symbol)`,
    [forexPairs.map(pair => pair.symbol_pair)]
  );
  return result; // Return all the latest prices
};

const calculatePL = async (fastify) => {
    try {
        const orders = await getOpenOrders(fastify);  // Fetch open orders from DB
        const latestPrices = await getAllLatestPrices(fastify);  // Fetch latest bid/ask prices
        
        // Calculate P/L for each order
        const updatedOrders = orders.map(order => {
            const latestPrice = latestPrices.find(price => price.symbol === order.symbol);

            if (!latestPrice) {
                console.error(`No latest price found for symbol: ${order.symbol}`);
                return order;  // If no price found, return order unchanged
            }

            // console.log('test', order)

            // Extract relevant data
            const { type, price, volume } = order;
            const currentBid = parseFloat(latestPrice.bid);
            const currentAsk = parseFloat(latestPrice.ask);
            const openPriceFloat = parseFloat(price);
            const lotSizeFloat = parseFloat(volume) || 0.01;  // Default to 1 if no lot size provided
            let pl = 0;

            // Calculate P/L based on order type (buy or sell)
            if (type === 'buy') {
                pl = (currentBid - openPriceFloat) * lotSizeFloat;
            } else if (type === 'sell') {
                pl = (openPriceFloat - currentAsk) * lotSizeFloat;
            }

            return {
                ...order,
                profit: pl.toFixed(2),  // Return profit rounded to 2 decimal places
            };
        });

        for (const order of updatedOrders) {
            // console.log(order)
            const { id, profit } = order;

            // console.log('id:', id, 'profit:', profit)

            // Update the order's profit in the database
            await fastify.mysql.query(
                'UPDATE orders SET profit = ? WHERE id = ?',
                [profit, id]
            );
        }

        return updatedOrders;  // Return the updated orders

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

import fastify from 'fastify';
import cron from 'node-cron';

export const checkFloatingProfit = async (fastify) => {
    const prices = await getAllLatestPrices(fastify); // Pass fastify here
    // const orders = await getOpenOrders(fastify); // Pass fastify here

    const data = JSON.stringify({ prices });
}

// latest bid and ask price
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

// Function to get open orders
const getOpenOrders = async (fastify) => {
    const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE status = "open"');
  
    return orders;
};

// function closing all order for user
const closeOrdersForUser = async (userId) => {
    // Logic to close all orders for a given user
    await fastify.mysql.query(`UPDATE orders SET status = 'closed', SET remark = 'burst' WHERE user_id = ? AND status = 'open'`, [userId]);
};

const calculateFloatingProfit = (order, currentPrice) => {
    // Make sure currentPrice is an object with symbol, bid, and ask prices
    if (!currentPrice || !currentPrice.bid || !currentPrice.ask) {
        console.error('Invalid price data for symbol:', order.symbol);
        return 0;
    }

    const price = order.type === 'buy' ? parseFloat(currentPrice.bid) : parseFloat(currentPrice.ask);
    
    if (order.type === 'buy') {
        return (price - order.price) * order.volume;
    } else {
        return (order.price - price) * order.volume;
    }
};


const checkUserOrders = async (fastify) => {
    // console.log('test')
    try {
      const [users] = await fastify.mysql.query('SELECT * FROM users');
     const latestPrices = await getAllLatestPrices(fastify);  // Fetch live prices for symbols
    
      for (const user of users) {
        const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE user_id = ? AND status = "open"', [user.id]);
        
        let totalFloatingProfit = 0;
        for (const order of orders) {
            
          const currentPrice = latestPrices.find(price => price.symbol === order.symbol);;  // Get live price for the order's symbol
        
          const floatingProfit = calculateFloatingProfit(order, currentPrice);
          totalFloatingProfit += floatingProfit;

        }

        // console.log('total floating', totalFloatingProfit);
  
        // If total floating profit is less than negative wallet balance, close all orders
        if (totalFloatingProfit < -user.wallet_balance) {

          await closeOrdersForUser(user.id);
        }
      }
    } catch (error) {
      console.error('Error checking orders:', error);
    }
};

// Schedule the cron job to run every minute
export const FloatingPLOrder = (fastify) => {
    cron.schedule('* * * * * *', async () => {
        await checkFloatingProfit(fastify);
        await checkUserOrders(fastify);
    });
};
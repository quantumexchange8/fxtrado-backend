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
    
      for (const user of users) {
        // Fetch all open orders for this user
        const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE user_id = ? AND status = "open"', [user.id]);
  
        // Sum the negative profit values
        let totalNegativeProfit = 0;
        for (const order of orders) {
          // Parse the profit to a number, and handle null or invalid profit values
          const profit = order.profit !== null && !isNaN(order.profit) ? parseFloat(order.profit) : 0; 
  
          if (profit < 0) {
            totalNegativeProfit += profit; // Add only negative profits (which are now numbers)
          }
        }

        // Fetch user's wallet balance
        const [wallets] = await fastify.mysql.query('SELECT * FROM wallets WHERE user_id = ?', [user.id]);
        const wallet = wallets[0]; // Assuming each user has one wallet

        // If total negative profit exceeds the user's wallet balance, close the orders
        if (Math.abs(totalNegativeProfit) > wallet.balance) {
          console.log(`User ${user.id} has exceeded their wallet balance. Closing orders...`);
  
          // Update the status of all this user's orders to 'closed'
          await fastify.mysql.query('UPDATE orders SET status = "closed" WHERE user_id = ? AND status = "open"', [user.id]);
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
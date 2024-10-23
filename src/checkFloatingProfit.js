import fastify from 'fastify';
import cron from 'node-cron';

export const checkFloatingProfit = async (fastify) => {
  try {
      const prices = await getAllLatestPrices(fastify); // Fetch latest prices
      await checkUserOrders(fastify, prices);           // Pass prices to user orders checker
  } catch (error) {
      console.error('Error checking floating profit:', error);
  }
}

// latest bid and ask price
const getAllLatestPrices = async (fastify) => {
  try {
      const [forexPairs] = await fastify.mysql.query('SELECT symbol_pair FROM forex_pairs WHERE status = "active"');

      if (!forexPairs.length) return [];

      const [result] = await fastify.mysql.query(
          `SELECT symbol, bid, ask 
           FROM fxtrado.ticks 
           WHERE symbol IN (?) 
           AND Date = (SELECT MAX(Date) FROM fxtrado.ticks WHERE symbol = fxtrado.ticks.symbol)`,
          [forexPairs.map(pair => pair.symbol_pair)]
      );

      return result; // Return all the latest prices
  } catch (error) {
      console.error('Error fetching latest prices:', error);
      return [];
  }
};

const calculateFloatingProfit = (order, currentPrice) => {
    // Make sure currentPrice is an object with symbol, bid, and ask prices
    if (!currentPrice || !currentPrice.bid || !currentPrice.ask) {
        console.error('Invalid price data for symbol:', order.symbol);
        return 0;
    }

    const price = order.type === 'buy' ? parseFloat(currentPrice.bid) : parseFloat(currentPrice.ask);
    const digits = currentPrice.digits;

    let decimal_digit = 10;
    if (digits === 5) {
      decimal_digit = 100000;
    } else if (digits === 3) {
      decimal_digit = 1000;
    } else {
      decimal_digit = 10;
    }

    if (order.type === 'buy') {
        return (price - order.price) * order.volume * decimal_digit;
    } else {
        return (order.price - price) * order.volume * decimal_digit;
    }
};


const checkUserOrders = async (fastify, prices) => {
    try {
      const [users] = await fastify.mysql.query('SELECT * FROM users');
    
      let orderProfit;
      for (const user of users) {
        // Fetch all open orders for this user
        const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE user_id = ? AND status = "open"', [user.id]);
  
        // If no orders, continue to the next user
        if (!orders.length) continue;

        // Sum the negative profit values
        let totalNegativeProfit = 0;
        for (const order of orders) {
          const currentPrice = prices.find(p => p.symbol === order.symbol);
          const floatingProfit = calculateFloatingProfit(order, currentPrice);
          if (floatingProfit < 0) totalNegativeProfit += floatingProfit;
      }

        // Fetch user's wallet balance
        const [wallets] = await fastify.mysql.query('SELECT * FROM wallets WHERE user_id = ?', [user.id]);
        const wallet = wallets[0]; // Assuming each user has one wallet

        // If total negative profit exceeds the user's wallet balance, close the orders
        if (Math.abs(totalNegativeProfit) > wallet.balance) {
          console.log(`User ${user.id} has exceeded their wallet balance. Closing orders...`);

          await closeUserOrders(fastify, user.id, totalNegativeProfit);

          // for (const order of orders) {
          //   // Calculate the close price based on market conditions
          //   let close_price;
          //   if (order.type === 'buy') {
          //     close_price = order.market_bid; // For buy orders, we sell at the bid price
          //   } else if (order.type === 'sell') {
          //     close_price = order.market_ask; // For sell orders, we buy at the ask price
          //   }
      
          //   // Calculate closed_profit
          //   const price_diff = (order.type === 'buy') ? (close_price - order.price) : (order.price - close_price);
          //   const closed_profit = price_diff * order.lot_size;
      
          //   // Update each order with close_price and closed_profit
          //   await fastify.mysql.query(`
          //     UPDATE orders 
          //     SET status = "closed", remark = "Burst", close_time = NOW(), close_price = ?, closed_profit = ?
          //     WHERE id = ?
          //   `, [close_price, closed_profit, order.id]);
          // }
  
          // Update the status of all this user's orders to 'closed'
          // await fastify.mysql.query('UPDATE orders SET status = "closed", remark = "Burst", closed_profit = ?, close_time = NOW() WHERE user_id = ? AND status = "open"', [orderProfit , user.id]);
          // await fastify.mysql.query('UPDATE wallets SET balance = 0.00 WHERE user_id = ? ', [user.id]);
        }
      }
    } catch (error) {
      console.error('Error checking orders:', error);
    }
};

const closeUserOrders = async (fastify, userId, totalNegativeProfit) => {
  try {
      const connection = await fastify.mysql.getConnection();
      await connection.beginTransaction();

      await connection.query(
          'UPDATE orders SET status = "closed", remark = "Burst", closed_profit = ?, close_time = NOW() WHERE user_id = ? AND status = "open"',
          [totalNegativeProfit, userId]
      );

      await connection.query('UPDATE wallets SET balance = 0.00 WHERE user_id = ?', [userId]);

      await connection.commit();
      connection.release();
  } catch (error) {
      console.error(`Error closing orders for user ${userId}:`, error);
      connection.rollback(); // Rollback transaction on failure
      connection.release();
  }
};


// Schedule the cron job to run every minute
export const FloatingPLOrder = (fastify) => {
    cron.schedule('* * * * * *', async () => {
        await checkFloatingProfit(fastify);
    });
};
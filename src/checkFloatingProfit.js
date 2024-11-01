import fastify from 'fastify';
import cron from 'node-cron';

// Main function to check floating profits
export const checkFloatingProfit = async (fastify) => {
    try {
        const prices = await getAllLatestPrices(fastify); // Fetch latest prices
        await checkUserOrders(fastify, prices);           // Pass prices to user orders checker
    } catch (error) {
        console.error('Error checking floating profit:', error);
    }
};

// Function to fetch the latest bid and ask prices
const getAllLatestPrices = async (fastify) => {
    try {
        const [forexPairs] = await fastify.mysql.query('SELECT symbol_pair FROM forex_pairs WHERE status = "active"');
        if (!forexPairs.length) return []; // Return empty array if no pairs

        const symbols = forexPairs.map(pair => pair.symbol_pair);

        // Fetch latest prices for the active forex pairs
        const [result] = await fastify.mysql.query(
            `SELECT symbol, bid, ask, digits
             FROM fxtrado.ticks 
             WHERE symbol IN (?) 
             AND Date = (SELECT MAX(Date) FROM fxtrado.ticks WHERE symbol = fxtrado.ticks.symbol)`,
            [symbols]
        );

        // Create a map for easy lookup
        const priceMap = new Map(result.map(p => [p.symbol, p]));
        return priceMap; // Return a map of the latest prices
    } catch (error) {
        console.error('Error fetching latest prices:', error);
        return new Map(); // Return an empty map in case of error
    }
};

// Function to calculate floating profit
const calculateFloatingProfit = (order, currentPrice) => {
    if (!currentPrice || typeof currentPrice.bid !== 'number' || typeof currentPrice.ask !== 'number') {
        console.error('Invalid price data for symbol:', order.symbol);
        return 0; // Return 0 if price data is invalid
    }

    const price = order.type === 'buy' ? currentPrice.bid : currentPrice.ask;
    const decimalDigit = currentPrice.digits === 5 ? 100000 : (currentPrice.digits === 3 ? 1000 : 10);

    // Calculate and return floating profit
    return (order.type === 'buy' ? (price - order.price) : (order.price - price)) * order.volume * decimalDigit;
};

// Function to check user orders
const checkUserOrders = async (fastify, prices) => {
    try {
        const [users] = await fastify.mysql.query('SELECT * FROM users');

        for (const user of users) {
            const [orders] = await fastify.mysql.query('SELECT * FROM orders WHERE user_id = ? AND status = "open"', [user.id]);
            if (!orders.length) continue; // If no orders, continue to the next user

            const totalNegativeProfit = await processOrders(orders, prices); // Calculate negative profits
            await handleNegativeProfit(fastify, user.id, totalNegativeProfit); // Handle negative profits
        }
    } catch (error) {
        console.error('Error checking orders:', error);
    }
};

// Function to process orders and calculate total negative profit
const processOrders = async (orders, priceMap) => {
    let totalNegativeProfit = 0;

    for (const order of orders) {
        const currentPrice = priceMap.get(order.symbol);
        const floatingProfit = calculateFloatingProfit(order, currentPrice);
        if (floatingProfit < 0) totalNegativeProfit += floatingProfit;
    }

    return totalNegativeProfit;
};

// Function to handle negative profits
const handleNegativeProfit = async (fastify, userId, totalNegativeProfit) => {
    const [wallets] = await fastify.mysql.query('SELECT * FROM wallets WHERE user_id = ?', [userId]);
    const wallet = wallets[0];

    // If total negative profit exceeds wallet balance, close orders
    if (Math.abs(totalNegativeProfit) > wallet.balance) {
        console.log(`User ${userId} has exceeded their wallet balance. Closing orders...`);
        await closeUserOrders(fastify, userId, totalNegativeProfit);
    }
};

// Function to close user orders and reset wallet balance
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
        await connection.rollback(); // Rollback transaction on failure
        connection.release();
    }
};

// Schedule the cron job to run every minute
export const FloatingPLOrder = (fastify) => {
    cron.schedule('* * * * *', async () => {
        await checkFloatingProfit(fastify);
    });
};

import Fastify from "fastify";
// import BuyController from './BuyController.js';
// import UserController from './UserController.js'
import fastifyMysql from "@fastify/mysql";
import axios from 'axios';
import websocket from '@fastify/websocket';
import cors from '@fastify/cors';
import { startVolumeCreation } from './cronJobs.js';
import { scheduleOpenOrderUpdates } from './orderCalculation.js';
import { FloatingPLOrder } from './checkFloatingProfit.js';
import Sensible from '@fastify/sensible'

const fastify = Fastify({
    logger: true
});

fastify.register(cors, {
  origin: ['http://127.0.0.1:8000', 'http://127.0.0.1:8010', 'https://fxtrado-user.currenttech.pro'], // Allow your frontend origins
  methods: ['GET', 'POST', 'PUT', 'DELETE'], // Allow methods used in your API
  credentials: true, // If your API requires credentials (cookies, HTTP authentication)
});

// Register the WebSocket plugin
fastify.register(websocket, {
  options: { maxPayload: 1048576 }
});

// API
const OANDA_API_KEY = 'cade200b33a840342cb1f08a79e2c5cd-ed020510413913fe81d60579a39711de'; 
const account_id = '101-003-30075838-001';
const instrument = 'EUR_USD'
// const OANDA_CANDLES = `https://api-fxpractice.oanda.com/v3/instruments/`;
const OANDA_PRICE_URL = `https://api-fxpractice.oanda.com/v3/accounts/${account_id}/pricing`

// Database Access
fastify.register(fastifyMysql, {
    host: '68.183.177.155',
    user: 'ctadmin',
    password: 'CTadmin!123',
    database: 'fxtrado',
    promise: true,
});

let exchangeRateData = {};

const fetchSymbols = async (connection) => {
  try {
    // Query to get base and quote from the forex_pairs table
    const [rows] = await connection.query('SELECT currency_pair, symbol_pair FROM forex_pairs WHERE status = "active"');
    
    // Map the result into the symbols array format
    return rows.map(row => ({
      currency_pair: row.currency_pair,
      symbol_pair: row.symbol_pair,
    }));
  } catch (error) {
    console.error('Error fetching forex pairs:', error);
    return [];
  }
};

const fetchExchangeRate = async () => {
  let connection;

  try {
    connection = await fastify.mysql.getConnection();
    const symbols = await fetchSymbols(connection);

    const currentDate = new Date();
    const date = currentDate.getUTCFullYear() + '-' +
                 String(currentDate.getUTCMonth() + 1).padStart(2, '0') + '-' +
                 String(currentDate.getUTCDate()).padStart(2, '0') + ' ' +
                 String(currentDate.getUTCHours()).padStart(2, '0') + ':' +
                 String(currentDate.getUTCMinutes()).padStart(2, '0') + ':' +
                 String(currentDate.getUTCSeconds()).padStart(2, '0') + '.' +
                 String(currentDate.getUTCMilliseconds()).padStart(3, '0');

    // Fetch data in parallel using `Promise.all`
    const fetchTasks = symbols.map(async (symbol) => {
      
      const { currency_pair } = symbol;
      const { symbol_pair } = symbol;

      try {
        const response = await axios.get(OANDA_PRICE_URL, {
          headers: { 
            'Authorization': `Bearer ${OANDA_API_KEY}`,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
          }, 
          params: {
            instruments: currency_pair
          }
        });

        // Logging the data
        const priceData = response.data.prices[0];
        const bid = priceData.bids[0].price;
        const ask = priceData.asks[0].price;

        // Additional information
        const instrument = priceData.instrument;
        const remark = 'OANDA';
        const symbolPair = symbol_pair;

        return [date, symbolPair, bid, ask, remark];
      } catch (error) {
        console.error(`Failed to fetch data for ${currency_pair}:`, error.message);
        return null; // Return null if the request fails
      }
    });

    // Resolve all the promises and filter out null responses
    const results = (await Promise.all(fetchTasks)).filter(result => result !== null);

    if (results.length > 0) {
      await connection.query(
        'INSERT INTO ticks (Date, Symbol, Bid, Ask, Remark) VALUES ?',
        [results]
      );
    }

  } catch (error) {
    console.error('Failed to fetch exchange rate:', error.message);
  } finally {
    if (connection) {
      connection.release();
    }
  }
};

fastify.register(async function (fastify, opts) {
  scheduleOpenOrderUpdates(fastify);
  FloatingPLOrder(fastify);
});

setInterval(fetchExchangeRate, 1000);

// TEMPORARY OFF FIRST
// const getLastMinuteCandle = async () => {
//   let connection;

//   try {
//     connection = await fastify.mysql.getConnection();
//     const symbols = await fetchSymbols(connection);

//     const currentDate = new Date();
//     const date = currentDate.getUTCFullYear() + '-' +
//                  String(currentDate.getUTCMonth() + 1).padStart(2, '0') + '-' +
//                  String(currentDate.getUTCDate()).padStart(2, '0') + ' ' +
//                  String(currentDate.getUTCHours()).padStart(2, '0') + ':' +
//                  String(currentDate.getUTCMinutes()).padStart(2, '0') + ':' +
//                  String(currentDate.getUTCSeconds()).padStart(2, '0') + '.' +
//                  String(currentDate.getUTCMilliseconds()).padStart(3, '0');

//     const fetchTasks = symbols.map(async (symbol) => {

//     const { currency_pair } = symbol;

//     try {
//       const response = await axios.get(`https://api-fxpractice.oanda.com/v3/instruments/${currency_pair}/candles`, {
//         headers: { 
//           'Authorization': `Bearer ${OANDA_API_KEY}`,
//           'Accept': 'application/json',
//           'Content-Type': 'application/json',
//         },
//         params: {
//           price: 'M', // Get bid/ask prices
//           granularity: 'M1', // 1-minute candles
//           count: 1 // Get the latest candle (set count to 1)
//         }
//       });

//       console.log('res', response.data.candles[0])
//       const instrument = response.data.instrument;
//       const candle = response.data.candles[0];
//       const open = candle.mid.o;
//       const high = candle.mid.h;
//       const low = candle.mid.l;
//       const close = candle.mid.c;
//       const symbolPair = instrument;
      
//       return [date, open, high, low, close], symbolPair;

//     } catch (error) {
//       console.error(`Failed to fetch data for ${currency_pair}:`, error.message);
//       return null; // Return null if the request fails
//     }
//   });

//   if (results.length > 0) {
//     await connection.query(
//       'INSERT INTO ticks (Date, Symbol, Bid, Ask, Remark) VALUES ?',
//       [results]
//     );
//   }

//   } catch (error) {
//     console.error(`Failed to fetch data for ${currency_pair}:`, error.message);
//   }
// }

// setInterval(getLastMinuteCandle, 60000);

fastify.register(async function (fastify) {

  fastify.get('/getOrder', { websocket: true }, async (socket, req) => {
    console.log('Client connected!');
    
    let lastSentOrders = []; // Cache to keep track of the last sent orders
    let interval;

    const fetchAndSendOrders = async () => {
        try {
            // Fetch orders from the database
            const [pOrders] = await fastify.mysql.query('SELECT * FROM orders WHERE status != "closed"');

            // Convert to string and compare with last sent orders to avoid redundant sends
            const ordersStr = JSON.stringify(pOrders);

            if (ordersStr !== JSON.stringify(lastSentOrders)) {
                // Send orders only if they have changed
                socket.send(ordersStr);
                lastSentOrders = pOrders; // Update the cache
            }
        } catch (err) {
            console.error('Error fetching orders:', err);
        }
    };

    // Periodically fetch and send orders
    interval = setInterval(fetchAndSendOrders, 1000); // Query every 3 seconds (adjust as needed)

    socket.on('close', () => {
        console.log('Client disconnected');
        clearInterval(interval); // Clear interval when the client disconnects
    });

    socket.on('error', (err) => {
        console.error('WebSocket error:', err);
        clearInterval(interval); // Clear interval on error to avoid memory leaks
    });
  });

  fastify.get('/floating-profit', { websocket: true }, (connection, req) => {
    console.log('Client connected');
    
    socket.on('close', () => {
      console.log('Client disconnected');
      clearInterval(interval); // Clear interval when the client disconnects
    });
  });


});

const getActiveForexPairs = async () => {
  const [forexPairs] = await fastify.mysql.query('SELECT currency_pair, symbol_pair FROM forex_pairs WHERE status = "active"');
  return forexPairs;
};

const getAllLatestPrices = async (forexPairs) => {
  const [result] = await fastify.mysql.query(
    `SELECT symbol, bid, ask 
     FROM ticks 
     WHERE symbol IN (?) 
     AND Date = (SELECT MAX(Date) FROM ticks WHERE symbol = ticks.symbol)`,
    [forexPairs.map(pair => pair.symbol_pair)]
  );
  
  return result;
};

const connectedClients = new Set();
fastify.register(async function (fastify) {

  fastify.get('/forex_pair', { websocket: true }, async (connection, req) => {
    console.log('Client connected!');
    connectedClients.add(connection);

    connection.on('open', () => {
      console.log('WebSocket connection opened');
      console.log('1', connection.readyState);
      console.log('2', connection.OPEN);
    });

    // Function to fetch and send the latest prices
    const sendPricesToClient = async () => {
      const forexPairs = await getActiveForexPairs(); // Fetch active pairs
      const latestPrices = await getAllLatestPrices(forexPairs); // Get latest prices

      connectedClients.forEach(client => {
        if (client.readyState === client.OPEN) {
          latestPrices.forEach(latestPrice => {
            client.send(
              JSON.stringify({
                symbol: latestPrice.symbol,
                bid: latestPrice.bid,
                ask: latestPrice.ask,
              })
            );
          });
        }
      });
    };

    // Initial price fetch and send
    await sendPricesToClient();

    // Set up the interval for sending updated prices
    const interval = setInterval(async () => {
      await sendPricesToClient(); // Send updated prices every second
    }, 1000);

    // Handle socket close event
    connection.on('close', () => {
      clearInterval(interval); // Clear the interval to avoid memory leaks
      connectedClients.delete(connection); // Remove the client from the set
      console.log('Client disconnected');
    });

    connection.on('error', (error) => {
      console.error('WebSocket error:', error);
      connectedClients.delete(connection);
    });
  });
});

const openOrderSchema = {
  body: {
    type: 'object',
    required: ['symbol', 'price', 'type'], // Fields required in the request body
    properties: {
      user_id: { type: 'string' },
      symbol: { type: 'string' },
      price: { type: 'number' },
      type: { type: 'string', enum: ['buy', 'sell'] }, // You can validate the action as 'buy' or 'sell'
      volume: { type: 'number' }
    }
  }
};

const closeOrderSchema = {
  body: {
    type: 'object',
    required: ['symbol', 'price', 'type', 'orderId', 'userId'],
    properties: {
      symbol: { type: 'string' },
      price: { type: 'number' },
      type: { type: 'string' },
      orderId: { type: 'string' }, 
      userId: { type: 'string' },
      marketPrice: { type: 'number' }
    }
  }
};

fastify.post('/api/openOrders', { schema: openOrderSchema }, async (request, reply) => {
  const { user_id, symbol, price, type, volume, status } = request.body;
  const currentDate = new Date(); 
  const open_time = formatDate(currentDate);

  const connection = await fastify.mysql.getConnection(); // Get a single connection

  try {
    // Begin transaction
    await connection.beginTransaction();

    // Step 1: Fetch the current running number with a FOR UPDATE lock
    const [orderRunningNum] = await connection.query('SELECT last_number, digits FROM running_numbers WHERE type = "order_opened" FOR UPDATE');
    
    let currentRunningNumber = parseInt(orderRunningNum[0].last_number);
    let rNumDigit = orderRunningNum[0].digits;
    const newRunningNumber = currentRunningNumber + 1;

    // Step 2: Generate the order_id with leading zeros based on the digits
    const order_id = String(newRunningNumber).padStart(rNumDigit, '0');

    // Step 3: Insert the order into the orders table
    const result = await connection.query(
      'INSERT INTO orders (user_id, symbol, price, type, volume, open_time, status, order_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [user_id, symbol, price, type, volume, open_time, status, order_id]
    );

    // Step 4: Update the running number in the running_numbers table
    await connection.query('UPDATE running_numbers SET last_number = ? WHERE type = "order_opened"', [newRunningNumber]);

    // Commit the transaction
    await connection.commit();

    // Step 5: Send the response back to the client
    reply.code(201).send({
      message: 'Order successfully placed',
      orderId: result[0].insertId, // You can return the inserted order ID
      orderNumber: order_id // Return the generated order ID as well
    });
  } catch (err) {
    // Rollback the transaction in case of an error
    await connection.rollback();

    // Log the error and send an error response
    fastify.log.error(err);
    reply.code(500).send({ message: 'Failed to place order', error: err.message });
  } finally {
    // Release the connection regardless of success or failure
    connection.release();
  }
});

fastify.post('/api/closeOrder', { schema: closeOrderSchema }, async (request, reply) => {
  const { userId, orderId, symbol, price, type, marketPrice } = request.body;
  
  const currentDate = new Date(); 
  const close_time = formatDate(currentDate);  // or use your custom formatDate function
  const status = 'closed';  // We will set the status to 'closed'

  try {
    // Get a connection from the MySQL pool
    const connection = await fastify.mysql.getConnection();

    // Step 1: Check if the order exists
    const [order] = await connection.query('SELECT * FROM orders WHERE order_id = ? AND user_id = ?', [orderId, userId]);

    const [user_wallet] = await connection.query('SELECT * FROM wallets WHERE user_id = ?', [userId]);
    const currentBalance = parseFloat(user_wallet[0].balance);
    
    const newBalance = currentBalance + price;
    
    if (order.length === 0) {
      // If no order is found, return an error
      reply.status(404).send({ error: 'Order not found' });
      return;
    }

    // Step 2: Update the order's status to 'closed'
    await connection.query(
      'UPDATE orders SET status = ?, close_price = ?, close_time = ?, profit = ? WHERE order_id = ? AND user_id = ?',
      [status, marketPrice, close_time, price, orderId, userId]
    );

    await connection.query(
      'UPDATE wallets SET balance = ? WHERE user_id = ?',
      [newBalance, userId]
    );

    // Release the connection
    connection.release();

    // Step 3: Return a success message
    reply.code(200).send({
      message: 'Order successfully closed',
      newBalance: newBalance, // Return the new wallet balance
    });
  } catch (error) {
    console.error('Error closing order:', error);
    reply.status(500).send({ error: 'An error occurred while closing the order' });
  }
});


const formatDate = (date) => {
  return date.getUTCFullYear() + '-' +
    String(date.getUTCMonth() + 1).padStart(2, '0') + '-' +
    String(date.getUTCDate()).padStart(2, '0') + ' ' +
    String(date.getUTCHours()).padStart(2, '0') + ':' +
    String(date.getUTCMinutes()).padStart(2, '0') + ':' +
    String(date.getUTCSeconds()).padStart(2, '0') + '.' +
    String(date.getUTCMilliseconds()).padStart(3, '0');
};

const start = async () => {
  try {
    await fastify.listen({ port: 3000 });
    fastify.log.info(`Server listening on ${fastify.server.address().port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();

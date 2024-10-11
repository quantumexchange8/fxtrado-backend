import Fastify from "fastify";
// import BuyController from './BuyController.js';
// import UserController from './UserController.js'
import fastifyMysql from "@fastify/mysql";
import axios from 'axios';
import websocket from '@fastify/websocket';
import cors from '@fastify/cors';
import { startVolumeCreation } from './cronJobs.js';
import { scheduleOpenOrderUpdates } from './orderCalculation.js';
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
fastify.register(websocket);

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
  fastify.get('/forex_pair', { websocket: true }, async (socket /* WebSocket */, req /* FastifyRequest */) => {
    console.log('Client connected!');

    // Fetch forex pairs from the forex_pairs table
    const [forexPairs] = await fastify.mysql.query('SELECT currency_pair, symbol_pair FROM forex_pairs WHERE status = "active"'); // Assuming fastify.db is your database connection
    
    // Function to get the latest bid and ask price for a specific pair
    const getAllLatestPrices = async () => {
      // Fetch the latest bid and ask for all pairs in a single query
      const [result] = await fastify.mysql.query(
        `SELECT symbol, bid, ask 
         FROM fxtrado.ticks 
         WHERE symbol IN (?) 
         AND Date = (SELECT MAX(Date) FROM fxtrado.ticks WHERE symbol = fxtrado.ticks.symbol)`,
        [forexPairs.map(pair => pair.symbol_pair)]
      );
    
      return result; // Return all the latest prices
    };
    

    // Periodically fetch and send the latest bid/ask prices to the client
    const interval = setInterval(async () => {
      try {
        const latestPrices = await getAllLatestPrices();
    
        // Send all the latest prices to the client
        for (const latestPrice of latestPrices) {
          socket.send(JSON.stringify({
            symbol: latestPrice.symbol,
            bid: latestPrice.bid,
            ask: latestPrice.ask,
          }));
        }
      } catch (err) {
        console.error('Error fetching prices:', err);
      }
    }, 1000); // Fetch every 1 second

    // Handle client disconnect
    socket.on('close', () => {
      console.log('Client disconnected');
      clearInterval(interval); // Clear interval when the client disconnects
    });
  });

  fastify.get('/getOrder', {websocket: true}, async (socket, req ) => {
    console.log('Client connected!');

    const [pOrders] = await fastify.mysql.query('SELECT * FROM orders WHERE status != "closed"');
    
    const interval = setInterval(async () => {
      try {
        socket.send(JSON.stringify({
          orders: pOrders,
        }));
      } catch (err) {
        console.error('Error fetching prices:', err);
      }
    }, 1000);

    socket.on('close', () => {
      console.log('Client disconnected');
      clearInterval(interval); // Clear interval when the client disconnects
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

import Fastify from "fastify";
// import BuyController from './BuyController.js';
// import UserController from './UserController.js'
import fastifyMysql from "@fastify/mysql";
import axios from 'axios';
import websocket from '@fastify/websocket';
import { startVolumeCreation } from './cronJobs.js';

const fastify = Fastify({
    logger: true
});

// Register the WebSocket plugin
fastify.register(websocket);

// API
const OANDA_API_KEY = 'e7b1a197-9540-4696-8330-f6cc625aedd5'; 
const OANDA_BASE_URL = 'https://exchange-rates-api.oanda.com';

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
    const [rows] = await connection.query('SELECT base, quote FROM forex_pairs WHERE status = "active" ');
    
    // Map the result into the symbols array format
    const symbols = rows.map(row => ({
      base: row.base,
      quote: row.quote
    }));

    return symbols;
  } catch (error) {
    console.error('Error fetching forex pairs:', error);
    return [];
  }
};

const fetchExchangeRate = async () => {
  try {

    const connection = await fastify.mysql.getConnection();
    const symbols = await fetchSymbols(connection);

    for (const symbol of symbols) {
      const { base, quote } = symbol;

      const response = await axios.get(`${OANDA_BASE_URL}/v2/rates/spot.json`, {
        headers: {
          'Authorization': `Bearer ${OANDA_API_KEY}`,
        },
        params: {
          'base': base,
          'quote': quote,
        },
      });
      
      // Extract the quotes data
      const quotes = response.data.quotes[0]; // Assuming there's always one quote
      const bid = quotes.bid;
      const ask = quotes.ask;
      const remark = 'OANDA';
      const symbols = `${symbol.base}/${symbol.quote}`;
      const currentDate = new Date();
      const date = currentDate.getUTCFullYear() + '-' +
             String(currentDate.getUTCMonth() + 1).padStart(2, '0') + '-' +
             String(currentDate.getUTCDate()).padStart(2, '0') + ' ' +
             String(currentDate.getUTCHours()).padStart(2, '0') + ':' +
             String(currentDate.getUTCMinutes()).padStart(2, '0') + ':' +
             String(currentDate.getUTCSeconds()).padStart(2, '0') + '.' +
             String(currentDate.getUTCMilliseconds()).padStart(3, '0');
               
      await connection.query(
        'INSERT INTO ticks (Date, Symbol, Bid, Ask, Remark) VALUES (?, ?, ?, ?, ?)',
        [date, symbols, bid, ask, remark]
      );

      connection.release();
    }

  } catch (error) {
    console.error('Failed to fetch exchange rate:', error.message);
  }
};

setInterval(fetchExchangeRate, 500);

fastify.get('/currentPrice', async (request, reply) => {
  if (!exchangeRateData || Object.keys(exchangeRateData).length === 0) {
    return reply.status(500).send({
      error: 'No data available',
    });
  }
  reply.send({ exchangeRateData });
});

startVolumeCreation(fastify);

fastify.register(async function (fastify) {
  fastify.get('/webSocket', { websocket: true }, async (socket /* WebSocket */, req /* FastifyRequest */) => {
    console.log('Client connected!');

    // Fetch forex pairs from the forex_pairs table
    const [forexPairs] = await fastify.mysql.query('SELECT currency_pair FROM forex_pairs WHERE status = "active"'); // Assuming fastify.db is your database connection

    // Function to get the latest bid and ask price for a specific pair
    const getLatestPrices = async (symbol) => {
      const result = await fastify.mysql.query(
        'SELECT bid, ask FROM fxtrado.ticks WHERE symbol = ? ORDER BY Date DESC LIMIT 1',
        [symbol]
      );

      return result[0]; // Return the latest tick
    };

    // Periodically fetch and send the latest bid/ask prices to the client
    const interval = setInterval(async () => {
      for (const pair of forexPairs) {
        const latestPrices = await getLatestPrices(pair.currency_pair); // Assuming 'symbol' is the column name in forex_pairs
        
        if (latestPrices && latestPrices.length > 0) {
          const latestPrice = latestPrices[0];

          socket.send(JSON.stringify({
            symbol: pair.currency_pair,
            bid: latestPrice.bid,
            ask: latestPrice.ask,
          }));
        }
      }
    }, 1000); // Fetch every 1 second

    // Handle client disconnect
    socket.on('close', () => {
      console.log('Client disconnected');
      clearInterval(interval); // Clear interval when the client disconnects
    });
  });
});


// fastify.get('/webSocket', { websocket: true }, (connection /* SocketStream */, req) => {
//   console.log('Client connected!');

//   // Send a welcome message
//   connection.write('Welcome to Fastify WebSocket!');

//   // Handle incoming messages from client
//   connection.on('message', (message) => {
//     console.log(`Received message from client: ${message}`);
    
//     // Echo the message back to the client
//     connection.write(`You said: ${message}`);
//   });

//   // Handle client disconnect
//   connection.on('close', () => {
//     console.log('Client disconnected');
//   });
// });

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

// fastify.register(BuyController, { prefix: '/buy' });
// fastify.register(UserController, { prefix: '/users' });

// fastify.get('/', (req, reply) => {
//     return {
//         message: 'Hello'
//     };
// });

// fastify.route({
//     method: 'GET',
//     url: '/hello/:name',
//     schema: {
//         querystring: {
//             properties: {
//                 lastName: { type: 'string' }
//             },
//             required: [ 'lastName' ]
//         },
//         params: {
//             properties: {
//                 name: { type: 'string' }
//             },
//             required: ['name']
//         },
//         response: {
//             200: {
//                 properties: {
//                     message: { type: 'string' }
//                 },
//                 required: ['message']
//             }
//         }
//     },
//     handler: (req, reply) => {
//         return {
//             message: `Hello ${req.params.name} ${req.query.lastName}`
//         }
//     }
// })

// try {
//     fastify.listen({ port: 3002 });
// } catch (error) {
//     fastify.log.error(error);
//     process.exit(1);
// }
import Fastify from "fastify";
// import BuyController from './BuyController.js';
// import UserController from './UserController.js'
import fastifyMysql from "@fastify/mysql";
import axios from 'axios';

const fastify = Fastify({
    logger: true
});

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
    const [rows] = await connection.query('SELECT base, quote FROM forex_pairs');
    
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
      const date = currentDate.getFullYear() + '-' +
               String(currentDate.getMonth() + 1).padStart(2, '0') + '-' +
               String(currentDate.getDate()).padStart(2, '0') + ' ' +
               String(currentDate.getHours()).padStart(2, '0') + ':' +
               String(currentDate.getMinutes()).padStart(2, '0') + ':' +
               String(currentDate.getSeconds()).padStart(2, '0') + '.' +
               String(currentDate.getMilliseconds()).padStart(3, '0');
               
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

setInterval(fetchExchangeRate, 300);

fastify.get('/currentPrice', async (request, reply) => {
  if (!exchangeRateData || Object.keys(exchangeRateData).length === 0) {
    return reply.status(500).send({
      error: 'No data available',
    });
  }
  reply.send({ exchangeRateData });
});

fastify.get('/getTicks', async (request, reply) => {
  try {
    // Get a connection to the MySQL database
    const connection = await fastify.mysql.getConnection();

    // Execute a query to retrieve all data from the 'ticks' table
    const [rows, fields] = await connection.query('SELECT * FROM ticks');

    // Release the connection back to the pool
    connection.release();

    // Send the fetched data as the response
    reply.send({
      success: true,
      data: rows
    });
  } catch (error) {
    // Handle any errors that occur during the query
    console.error('Error fetching ticks data:', error);
    reply.status(500).send({
      success: false,
      message: 'Failed to fetch ticks data',
      error: error.message
    });
  }
});


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
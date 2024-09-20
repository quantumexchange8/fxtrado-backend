const responseSchema = {
    response: {
        200: {
            properties: {
                message: { type: 'string' }
            }
        }
    }
};

const BuyController = (fastify, options, done) => {
    fastify.get('/', { schema: responseSchema}, (req, reply) => {
        return {
            message: 'Hello Buy'
        };
    });

    fastify.get('/:name', { schema: responseSchema}, (req, reply) => {
        return {
            message: `Hello ${req.params.name}`
        }
    })

    done();
};

export default BuyController;
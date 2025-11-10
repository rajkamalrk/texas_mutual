import { initDbPool } from './db-config.mjs';

let connection;

const setupConnection = async () => {
    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};

export const handler = async (event, context) => {
    // let connection;

    try {
        await setupConnection();

        // Check if a batch size is provided
        const batchSize = event.batchSize || 1; // Default to 1 if not provided
        const providerNumbers = [];

        for (let i = 0; i < batchSize; i++) {
            let [result] = await connection.query(`SELECT * FROM ${process.env.DB_1}.${process.env.TABLE_1} ORDER BY generated_provider_number DESC LIMIT 1`);

            console.log('result', result);
            let finalNumber;
            if (result.length == 0){
                if (process.env.ENVIRONMENT == 'dev'){
                    finalNumber = 660000000001
                }
                else if(process.env.ENVIRONMENT == 'qa'){
                    finalNumber = 770000000001
                }
                else if (process.env.ENVIRONMENT == 'stg'){
                    finalNumber = 880000000001
                }
                else if (process.env.ENVIRONMENT == 'prod'){
                    finalNumber = 990000000001
                }
            }else{
                finalNumber = result[0].generated_provider_number + 1;
            }

            console.log('finalNumber', finalNumber);

            // Insert the generated provider number into the database
            await connection.query(`INSERT INTO ${process.env.DB_1}.${process.env.TABLE_1} (generated_provider_number) VALUES (?)`, [finalNumber]);
            providerNumbers.push(finalNumber);
        }

        await connection.commit();
        console.log('batchSize', batchSize);
        if (batchSize > 1){
          return {
            statusCode: 200,
            body: {
                status: "success",
                message: "Provider numbers created successfully.",
                data: {
                    providerNumbers
                },
                transactionId: context.awsRequestId
            },
          };
        }else{
          return {
            statusCode: 200,
            body: {
                status: "success",
                message: "Provider number created successfully.",
                data: {
                    "providerNumber": providerNumbers[0]
                },
                transactionId: context.awsRequestId
            },
        };
        }

        

    } catch (error) {
        console.log('Error occurred', error);
        return {
            statusCode: 500, // Internal server error
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Internal server error',
                errorDescription: error.message,
                transactionId: context.awsRequestId
            }),
        };
    } finally {
        if (connection) {
            connection.release(); // Ensure the connection is released back to the pool
        }
    }
};


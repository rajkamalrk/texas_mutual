import mysql from 'mysql2/promise';
import {
    SecretsManagerClient,
    GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";

let connectionPool;

const getSecretDetails = async (secretName) => {
    const client = new SecretsManagerClient({ region: process.env.REGION });
    try {
        const response = await client.send(
            new GetSecretValueCommand({
                SecretId: secretName,
                VersionStage: "AWSCURRENT",
            })
        );
        return JSON.parse(response.SecretString);
    } catch (error) {
        console.error('Error retrieving secrets:', error);
        throw error;
    }
};

export const initDbPool = async () => {
    if (!connectionPool) {
        const secretDetails = await getSecretDetails(process.env.SECRET_NAME);
        connectionPool = mysql.createPool({
            host: secretDetails.host,
            user: secretDetails.username,
            password: secretDetails.password,
            database: secretDetails.dbClusterIdentifier,
        });
        console.log("DB connection pool initialized.");
    }
    return connectionPool;
};
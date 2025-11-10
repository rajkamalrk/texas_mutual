import { initDbPool } from './db-config.mjs';

let connection;
const setupConnection = async () => {
    console.log(connection, 'connection');

    if (!connection) {
        const pool = await initDbPool();
        connection = await pool.getConnection();
        console.log("DB connection initialized.");
    }
};

export const handler = async (event, context) => {
    try {
        await setupConnection();

        console.log(event, '=======> event');

        const eventData = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
        console.log("event =>", eventData);

        const now = new Date();
        const updatedOn = now.toISOString().slice(0, 19).replace('T', ' ');
        console.log(updatedOn, "updatedOn...");

        const validateData = validateEventData(eventData);

        if (validateData.length === 0) {
            if (!eventData.vendorNumber) {
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        status: 'error',
                        errorMessage: 'Vendor number can not be null',
                        errorDescription: 'Vendor number is required',
                        transactionId: context.awsRequestId,
                    }),
                };
            }
            const [result] = await connection.query(`
                SELECT 
                *
                FROM 
                 ${process.env.DB_1}.${process.env.TABLE_1} WHERE vendor_number = ?
                `, [eventData.vendorNumber]);

            if (result.length > 0) {
                console.log("result =>", result);

                let updateQuery = `UPDATE ${process.env.DB_1}.${process.env.TABLE_1} 
                SET 
                    vendor_tax_id = ?,
                    vendor_name = ?,
                    vendor_dba = ?,
                    vendor_status = ?,
                    vendor_address1 = ?,
                    vendor_address2 = ?,
                    vendor_city = ?,
                    vendor_state = ?,
                    vendor_zip_code = ?,
                    vendor_phone = ?,
                    delivered_to_sa = 'UPDATED',
                    update_user = 'LAMBDA', 
                    update_timestamp = ?
                WHERE vendor_number = ? 
                    `;

                let updateValues = [
                    eventData.taxID,
                    eventData.vendorName,
                    eventData.dba,
                    eventData.vendorStatus,
                    eventData.address.line1,
                    eventData.address.line2,
                    eventData.address.city,
                    eventData.address.state,
                    eventData.address.zipCode,
                    eventData.contact?.phone,
                    updatedOn,
                    eventData.vendorNumber
                ];

                const [updateResult] = await connection.query(updateQuery, updateValues);

                if (updateResult.affectedRows > 0) {
                    return {
                        statusCode: 200,
                        body: JSON.stringify({
                            status: 'success',
                            message: 'Vendor details updated successfully',
                            data: { vendorNumber: eventData.vendorNumber },
                            transactionId: context.awsRequestId,
                        }),
                    };
                } else {
                    return {
                        statusCode: 500,
                        body: JSON.stringify({
                            status: 'error',
                            errorMessage: 'Update operation failed',
                            errorDescription: 'No rows were affected by the update operation',
                            transactionId: context.awsRequestId,
                        }),
                    };
                }
            } else {
                return {
                    statusCode: 200,
                    body: JSON.stringify({
                        status: 'success',
                        message: 'Vendor does not exist in BITX for Update',
                        data: { vendorNumber: eventData.vendorNumber },
                        transactionId: context.awsRequestId,
                    }),
                };
            }
        } else {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    status: 'error',
                    errorMessage: `${validateData} are required fields`,
                    errorDescription: `${validateData} fields are missing in the payload`,
                    transactionId: context.awsRequestId,
                }),
            };
        }

    } catch (error) {
        console.log('Érror:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                status: 'error',
                errorMessage: 'Failed to update data',
                errorDescription: 'Failed to update data',
                transactionId: context.awsRequestId,
            }),
        };
    }
};

const validateEventData = (eventDate) => {

    const validateTopFields = [
        'taxID',
        'vendorName',
        'dba',
        'vendorNumber',
        'vendorStatus'
    ];

    const validateAddressFields = [
        'line1',
        'line2',
        'city',
        'state',
        'zipCode'
    ];

    const validateContactField = [
        'phone'
    ];

    const missingFields = [];

    validateTopFields.forEach(field => {
        if (!(field in eventDate)) {
            missingFields.push(field);
        }
    });

    if (eventDate.address) {
        validateAddressFields.forEach(field => {
            if (!(field in eventDate.address)) {
                missingFields.push(`${field}`);
            }
        });
    } else {
        missingFields.push('address');
    }

    if (eventDate.contact) {
        validateContactField.forEach(field => {
            if (!(field in eventDate.contact)) {
                missingFields.push(`${field}`);
            }
        });
    } else {
        missingFields.push('contact');
    }

    if (missingFields.length > 0) {
        console.log('Missing required fields:', missingFields);
        return missingFields;
    }

    return missingFields;
}